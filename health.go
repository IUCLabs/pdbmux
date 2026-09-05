package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// probeBodyLimit caps the status body read; only the per-service state is used.
const probeBodyLimit = 1 << 20

// runningState is the healthy state reported by PuppetDB's status service.
const runningState = "running"

const (
	stateHealthy          = "healthy"
	stateUnhealthy        = "unhealthy"
	stateUnprobed         = "unprobed"
	stateUnmonitored      = "unmonitored"
	stateProbeUnsupported = "probe_unsupported"
)

// probeOutcome is what one probe reply said. A rejected probe is deliberately
// neither a success nor a failure: it is evidence about the probe request, not
// about the backend behind it.
type probeOutcome int

const (
	outcomeOK probeOutcome = iota
	outcomeFailure
	outcomeRejected
)

type probeState struct {
	healthy     bool
	unsupported bool
	probed      bool
	outcome     probeOutcome
	failures    int
	successes   int
	lastProbe   time.Time
	lastErr     string
}

// backendHealth is a copy of one backend's probe state, safe to read outside the
// prober's lock.
type backendHealth struct {
	Healthy     bool
	Unsupported bool
	Probed      bool
	Failures    int
	Successes   int
	LastProbe   time.Time
	LastErr     string
}

func (h backendHealth) stateName() string {
	switch {
	case !h.Probed:
		return stateUnprobed
	case h.Unsupported:
		return stateProbeUnsupported
	case h.Healthy:
		return stateHealthy
	default:
		return stateUnhealthy
	}
}

// probeRejectedError marks a reply that refused the probe request itself rather
// than reporting the backend unwell.
type probeRejectedError struct {
	status int
	path   string
}

func (e *probeRejectedError) Error() string {
	return fmt.Sprintf("HTTP %d: probe path %q is not usable on this backend", e.status, e.path)
}

// probeRejects reports whether a status code refuses the probe request rather
// than reporting the service unhealthy. Every 4xx is the backend answering that
// our request is the problem, and 501 says it does not implement the endpoint.
// Other 5xx stay real evidence: trapperkeeper answers 503 exactly when its
// services are not nominal.
func probeRejects(code int) bool {
	return (code >= 400 && code < 500) || code == http.StatusNotImplemented
}

func classifyProbe(err error) probeOutcome {
	switch {
	case err == nil:
		return outcomeOK
	case errors.As(err, new(*probeRejectedError)):
		return outcomeRejected
	default:
		return outcomeFailure
	}
}

// prober polls each backend's health endpoint on an interval and tracks whether
// it is currently answering. A nil *prober means probing is disabled and every
// method is a no-op reporting every backend healthy.
type prober struct {
	backends  []Backend
	path      string
	interval  time.Duration
	failures  int
	successes int
	client    *http.Client
	log       *log.Logger
	now       func() time.Time

	mu     sync.Mutex
	states map[string]*probeState

	wg       sync.WaitGroup
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func newProber(cfg Config, logger *log.Logger) *prober {
	if !cfg.HealthProbe {
		return nil
	}
	p := &prober{
		backends:  append([]Backend(nil), cfg.Backends...),
		path:      cfg.HealthProbePath,
		interval:  cfg.HealthProbeInterval,
		failures:  cfg.HealthProbeFailures,
		successes: cfg.HealthProbeSuccesses,
		client:    &http.Client{Timeout: cfg.HealthProbeTimeout},
		log:       logger,
		now:       time.Now,
		states:    make(map[string]*probeState, len(cfg.Backends)),
	}
	// A backend nobody has probed yet counts as healthy, so a restart never
	// drops traffic while the first round runs.
	for _, b := range cfg.Backends {
		p.states[b.Name] = &probeState{healthy: true}
	}
	return p
}

// Start launches one polling goroutine per backend and returns immediately.
func (p *prober) Start(ctx context.Context) {
	if p == nil {
		return
	}
	ctx, p.cancel = context.WithCancel(ctx)
	for _, b := range p.backends {
		p.wg.Add(1)
		go p.loop(ctx, b)
	}
}

// Stop cancels the polling goroutines and waits for them to exit.
func (p *prober) Stop() {
	if p == nil || p.cancel == nil {
		return
	}
	p.stopOnce.Do(p.cancel)
	p.wg.Wait()
	p.client.CloseIdleConnections()
}

func (p *prober) loop(ctx context.Context, b Backend) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		p.probeOne(ctx, b)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// healthy reports whether queries should go to this backend. An unknown name is
// healthy so a config the prober does not know about is never black-holed.
func (p *prober) healthy(name string) bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.states[name]
	if !ok {
		return true
	}
	return st.healthy
}

func (p *prober) snapshot() map[string]backendHealth {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]backendHealth, len(p.states))
	for name, st := range p.states {
		out[name] = backendHealth{
			Healthy:     st.healthy,
			Unsupported: st.unsupported,
			Probed:      st.probed,
			Failures:    st.failures,
			Successes:   st.successes,
			LastProbe:   st.lastProbe,
			LastErr:     st.lastErr,
		}
	}
	return out
}

func (p *prober) probeOne(ctx context.Context, b Backend) {
	err := p.probe(ctx, b)
	// A probe cut short by shutdown says nothing about the backend.
	if ctx.Err() != nil {
		return
	}
	p.record(b.Name, err)
}

// record applies one probe outcome, flipping state only once the matching
// threshold is reached so a single blip cannot flap a backend in either
// direction.
func (p *prober) record(name string, err error) {
	p.mu.Lock()
	st, ok := p.states[name]
	if !ok {
		p.mu.Unlock()
		return
	}
	outcome := classifyProbe(err)
	// A run only counts while the outcome keeps its kind, so a rejected-probe run
	// and a real-failure run never add up to one threshold.
	if outcome != st.outcome {
		st.failures, st.successes = 0, 0
	}
	st.outcome = outcome
	st.probed = true
	st.lastProbe = p.now()

	var transition string
	switch outcome {
	case outcomeOK:
		st.lastErr = ""
		st.successes++
		if st.successes >= p.successes {
			switch {
			case st.unsupported:
				st.unsupported = false
				transition = fmt.Sprintf("info: backend %q health probe %q is usable again", name, p.path)
			case !st.healthy:
				st.healthy = true
				transition = fmt.Sprintf("info: backend %q is up after %d consecutive probe successes", name, st.successes)
			}
		}
	case outcomeRejected:
		st.lastErr = err.Error()
		st.failures++
		if !st.unsupported && st.failures >= p.failures {
			// The backend answered and refused the probe request itself, so this says
			// nothing about its health: it keeps serving queries, unverified.
			st.unsupported, st.healthy = true, true
			transition = fmt.Sprintf("warning: backend %q rejected the health probe %d times: %v; leaving it in service but unverified, fix health_probe_path or its authorization", name, st.failures, err)
		}
	default:
		st.lastErr = err.Error()
		st.failures++
		if st.failures >= p.failures {
			st.unsupported = false
			if st.healthy {
				st.healthy = false
				transition = fmt.Sprintf("warning: backend %q is down after %d consecutive probe failures: %v", name, st.failures, err)
			}
		}
	}
	p.mu.Unlock()

	// Only transitions are logged: this loop runs for the life of the process.
	if transition != "" {
		p.log.Print(transition)
	}
}

func (p *prober) probe(ctx context.Context, b Backend) error {
	target := strings.TrimRight(b.URL, "/") + p.path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, probeBodyLimit))
	if err != nil {
		return err
	}
	if probeRejects(resp.StatusCode) {
		return &probeRejectedError{status: resp.StatusCode, path: p.path}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return statusBodyHealthy(body)
}

// statusBodyHealthy rejects a 200 whose body reports a service that is starting,
// stopping or in error. A body that does not speak the status service's shape is
// accepted on its status code alone, so a custom health path still works.
func statusBodyHealthy(body []byte) error {
	var services map[string]struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &services); err != nil {
		return nil
	}
	for name, svc := range services {
		if svc.State != "" && svc.State != runningState {
			return fmt.Errorf("service %q is %q, not %q", name, svc.State, runningState)
		}
	}
	return nil
}

// partialTracker records whether the last merged fan-out heard from every
// configured backend.
type partialTracker struct {
	mu          sync.Mutex
	seen        bool
	contributed int
	configured  int
	rounds      uint64
	last        time.Time
}

func (t *partialTracker) record(contributed, configured int, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seen = true
	t.contributed, t.configured = contributed, configured
	if contributed < configured {
		t.rounds++
		t.last = now
	}
}

func (t *partialTracker) snapshot() (seen bool, contributed, configured int, rounds uint64, last time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen, t.contributed, t.configured, t.rounds, t.last
}
