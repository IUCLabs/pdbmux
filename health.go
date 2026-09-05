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
	healthy bool
	probed  bool
	// usable latches the first time the probe endpoint answers with a verdict on
	// this backend. It never clears: whether the probe works for a backend is a
	// property of the deployment, not something to re-decide every round.
	usable bool
	// rejecting tracks whether the last probe refused the request, so a rejection
	// run is logged when it starts rather than on every probe.
	rejecting bool
	// warned records that the unusable-probe misconfiguration has been logged.
	warned    bool
	failures  int
	successes int
	lastProbe time.Time
	lastErr   string
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

// probeAnsweredError marks a failure the probe endpoint itself reported: the
// request reached it and came back with a verdict on the backend's services.
type probeAnsweredError struct{ msg string }

func (e *probeAnsweredError) Error() string { return e.msg }

// probeAnswered reports whether a probe reply is a verdict we can read as
// healthy or unhealthy, which is what makes the probe usable for a backend. A
// rejection refused the request and a transport failure never reached the
// endpoint, so neither one shows that the configured path works.
func probeAnswered(err error) bool {
	return err == nil || errors.As(err, new(*probeAnsweredError))
}

// probeRejects reports whether a status code refuses the probe request rather
// than reporting the service unhealthy. A 4xx is the backend answering that our
// request is the problem, and 501 says it does not implement the endpoint.
// 429 is excluded: it is the backend reporting its own capacity, not a verdict
// on the request, so an overloaded backend must get backed off rather than kept
// at full traffic. Other 5xx stay real evidence too: trapperkeeper answers 503
// exactly when its services are not nominal.
func probeRejects(code int) bool {
	if code == http.StatusTooManyRequests {
		return false
	}
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
			Unsupported: st.probed && !st.usable,
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

// record applies one probe outcome. Which rule applies is decided by a latch,
// not by anything that decays: until the probe endpoint has answered once, this
// backend has no health signal at all, so nothing may gate on one and it stays
// in service however its probes fail. Once the probe has answered, the path
// works and every unsuccessful probe counts toward the failure threshold —
// rejections included, since a path that answered before and refuses now has
// changed. The latch never clears, so a repeating pattern of failures cannot
// argue a backend back into service.
func (p *prober) record(name string, err error) {
	p.mu.Lock()
	st, ok := p.states[name]
	if !ok {
		p.mu.Unlock()
		return
	}
	outcome := classifyProbe(err)
	st.probed = true
	st.lastProbe = p.now()

	var transitions []string
	if !st.usable && probeAnswered(err) {
		st.usable = true
		// Only evidence from the working probe debounces the state.
		st.failures, st.successes = 0, 0
		if st.warned {
			st.warned = false
			transitions = append(transitions, fmt.Sprintf("info: backend %q now answers the health probe %q; its health is being checked again", name, p.path))
		}
	}

	if outcome == outcomeOK {
		st.lastErr = ""
		st.failures = 0
		st.successes++
		if st.usable && !st.healthy && st.successes >= p.successes {
			st.healthy = true
			transitions = append(transitions, fmt.Sprintf("info: backend %q is up after %d consecutive probe successes", name, st.successes))
		}
	} else {
		st.lastErr = err.Error()
		st.successes = 0
		st.failures++
		if !st.usable {
			// No probe has ever worked here, so this says nothing about the backend: it
			// keeps serving queries, unverified.
			if st.failures >= p.failures && !st.warned {
				st.warned = true
				transitions = append(transitions, fmt.Sprintf("warning: backend %q has never answered the health probe %q, last %v; leaving it in service but unverified, fix health_probe_path or its authorization", name, p.path, err))
			}
		} else {
			if outcome == outcomeRejected && !st.rejecting {
				transitions = append(transitions, fmt.Sprintf("warning: backend %q now rejects the health probe %q, which worked before: %v; the endpoint moved or its authorization changed, so the refusals count as failures", name, p.path, err))
			}
			if st.healthy && st.failures >= p.failures {
				st.healthy = false
				transitions = append(transitions, fmt.Sprintf("warning: backend %q is down after %d consecutive failed probes: %v", name, st.failures, err))
			}
		}
	}
	st.rejecting = outcome == outcomeRejected
	p.mu.Unlock()

	// Only transitions are logged: this loop runs for the life of the process.
	for _, t := range transitions {
		p.log.Print(t)
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
		return &probeAnsweredError{msg: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))}
	}
	if err := statusBodyHealthy(body); err != nil {
		return &probeAnsweredError{msg: err.Error()}
	}
	return nil
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
