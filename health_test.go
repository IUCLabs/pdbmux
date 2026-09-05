package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const runningStatusBody = `{"puppetdb-status":{"service_version":"8.0.0","service_status_version":1,` +
	`"detail_level":"info","state":"running","status":{},"active_alerts":[]},` +
	`"status-service":{"service_version":"1.1.1","service_status_version":1,` +
	`"detail_level":"info","state":"running","status":{},"active_alerts":[]}}`

func degradedStatusBody(state string) string {
	return `{"puppetdb-status":{"service_version":"8.0.0","service_status_version":1,` +
		`"detail_level":"info","state":"` + state + `","status":{},"active_alerts":[]}}`
}

// statusBackend is a PuppetDB status endpoint whose reply the test can change
// between probes.
type statusBackend struct {
	srv *httptest.Server

	mu     sync.Mutex
	status int
	body   string
}

func newStatusBackend(t *testing.T) *statusBackend {
	t.Helper()
	sb := &statusBackend{status: http.StatusOK, body: runningStatusBody}
	sb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sb.mu.Lock()
		code, body := sb.status, sb.body
		sb.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(sb.srv.Close)
	return sb
}

func (sb *statusBackend) reply(status int, body string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.status, sb.body = status, body
}

func newTestProber(t *testing.T, cfg Config) *prober {
	t.Helper()
	p := newProber(cfg, log.New(io.Discard, "", 0))
	if p == nil {
		t.Fatal("newProber returned nil for an enabled config")
	}
	return p
}

func healthConfig(aURL, bURL string) Config {
	cfg := testConfig(aURL, bURL, mergeStatic)
	cfg.HealthProbe = true
	cfg.HealthProbePath = defaultHealthProbePath
	cfg.HealthProbeInterval = 5 * time.Millisecond
	cfg.HealthProbeTimeout = 2 * time.Second
	cfg.HealthProbeFailures = 2
	cfg.HealthProbeSuccesses = 2
	return cfg
}

func markDown(t *testing.T, p *prober, name string) {
	t.Helper()
	for i := 0; i < p.failures; i++ {
		p.record(name, errors.New("probe failed"))
	}
	if p.healthy(name) {
		t.Fatalf("backend %q still healthy after %d probe failures", name, p.failures)
	}
}

func markUnsupported(t *testing.T, p *prober, name string) {
	t.Helper()
	for i := 0; i < p.failures; i++ {
		p.record(name, &probeRejectedError{status: http.StatusNotFound, path: p.path})
	}
	if !p.healthy(name) {
		t.Fatalf("backend %q was excluded by a rejected probe", name)
	}
	if state := p.snapshot()[name].stateName(); state != stateProbeUnsupported {
		t.Fatalf("state = %q, want %q", state, stateProbeUnsupported)
	}
}

func TestProber_FailureThresholdDebouncesABlip(t *testing.T) {
	cfg := healthConfig("http://a.invalid", "http://b.invalid")
	cfg.HealthProbeFailures = 3
	p := newTestProber(t, cfg)

	for i := 1; i < 3; i++ {
		p.record("a", errors.New("boom"))
		if !p.healthy("a") {
			t.Fatalf("backend went down after %d of 3 failures", i)
		}
	}
	// A success inside the window resets the run, so the blip never flaps it out.
	p.record("a", nil)
	p.record("a", errors.New("boom"))
	p.record("a", errors.New("boom"))
	if !p.healthy("a") {
		t.Fatal("a failure run interrupted by a success should not mark the backend down")
	}
	p.record("a", errors.New("boom"))
	if p.healthy("a") {
		t.Fatal("backend should be down after 3 consecutive failures")
	}
}

func TestProber_SuccessThresholdDebouncesRecovery(t *testing.T) {
	cfg := healthConfig("http://a.invalid", "http://b.invalid")
	cfg.HealthProbeSuccesses = 2
	p := newTestProber(t, cfg)
	markDown(t, p, "a")

	p.record("a", nil)
	if p.healthy("a") {
		t.Fatal("one success should not bring a down backend back")
	}
	// A failure in between restarts the recovery run.
	p.record("a", errors.New("boom"))
	p.record("a", nil)
	if p.healthy("a") {
		t.Fatal("recovery run should restart after an interleaved failure")
	}
	p.record("a", nil)
	if !p.healthy("a") {
		t.Fatal("backend should be back up after 2 consecutive successes")
	}
}

func TestProber_UnprobedBackendIsHealthy(t *testing.T) {
	p := newTestProber(t, healthConfig("http://a.invalid", "http://b.invalid"))
	if !p.healthy("a") || !p.healthy("b") {
		t.Fatal("a backend nobody has probed yet must count as healthy")
	}
	if !p.healthy("not-configured") {
		t.Fatal("an unknown backend must count as healthy")
	}
	if state := p.snapshot()["a"].stateName(); state != stateUnprobed {
		t.Fatalf("state = %q, want %q", state, stateUnprobed)
	}
}

func TestProber_ProbeAcceptsRunningStatus(t *testing.T) {
	sb := newStatusBackend(t)
	p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))
	if err := p.probe(t.Context(), Backend{Name: "a", URL: sb.srv.URL}); err != nil {
		t.Fatalf("running status rejected: %v", err)
	}
}

func TestProber_ProbeRejectsDegradedBodyOnA200(t *testing.T) {
	sb := newStatusBackend(t)
	p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))
	for _, state := range []string{"starting", "error", "stopping", "unknown"} {
		sb.reply(http.StatusOK, degradedStatusBody(state))
		err := p.probe(t.Context(), Backend{Name: "a", URL: sb.srv.URL})
		if err == nil {
			t.Fatalf("state %q on a 200 was accepted as healthy", state)
		}
		if !strings.Contains(err.Error(), state) {
			t.Errorf("error %q does not name the state %q", err, state)
		}
	}
}

func TestProber_ProbeRejectsNon200(t *testing.T) {
	sb := newStatusBackend(t)
	sb.reply(http.StatusServiceUnavailable, degradedStatusBody("error"))
	p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))
	if err := p.probe(t.Context(), Backend{Name: "a", URL: sb.srv.URL}); err == nil {
		t.Fatal("503 was accepted as healthy")
	}
}

// A deployment pointing health_probe_path at something that is not the status
// service still works: the status code decides.
func TestProber_ProbeAcceptsNonStatusBody(t *testing.T) {
	sb := newStatusBackend(t)
	sb.reply(http.StatusOK, `running`)
	p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))
	if err := p.probe(t.Context(), Backend{Name: "a", URL: sb.srv.URL}); err != nil {
		t.Fatalf("non-status 200 body rejected: %v", err)
	}
}

func TestProber_ProbesUseTheConfiguredPath(t *testing.T) {
	var gotPath string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotPath = r.URL.Path
		mu.Unlock()
		_, _ = io.WriteString(w, runningStatusBody)
	}))
	t.Cleanup(srv.Close)

	cfg := healthConfig(srv.URL, "http://b.invalid")
	cfg.HealthProbePath = "/custom/health"
	p := newTestProber(t, cfg)
	if err := p.probe(t.Context(), Backend{Name: "a", URL: srv.URL + "/"}); err != nil {
		t.Fatalf("probe: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/custom/health" {
		t.Fatalf("probed %q, want /custom/health", gotPath)
	}
}

// The loop keeps probing a backend it has marked down, so recovery needs no
// operator action.
func TestProber_RecoversWithoutIntervention(t *testing.T) {
	sb := newStatusBackend(t)
	sb.reply(http.StatusServiceUnavailable, degradedStatusBody("error"))
	p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))

	p.Start(t.Context())
	defer p.Stop()

	waitFor(t, func() bool { return !p.healthy("a") })
	sb.reply(http.StatusOK, runningStatusBody)
	waitFor(t, func() bool { return p.healthy("a") })
}

func TestProber_StopLeavesNoGoroutines(t *testing.T) {
	sb := newStatusBackend(t)
	cfg := healthConfig(sb.srv.URL, sb.srv.URL)
	cfg.Backends = []Backend{
		{Name: "a", URL: sb.srv.URL},
		{Name: "b", URL: sb.srv.URL},
		{Name: "c", URL: sb.srv.URL},
	}
	p := newTestProber(t, cfg)

	settle := func() int {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	before := settle()

	p.Start(context.Background())
	waitFor(t, func() bool {
		snap := p.snapshot()
		return snap["a"].Probed && snap["b"].Probed && snap["c"].Probed
	})
	p.Stop()

	deadline := time.Now().Add(5 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		if after = settle(); after <= before {
			return
		}
	}
	t.Fatalf("goroutines did not settle after Stop: %d before, %d after", before, after)
}

func TestProber_StopIsIdempotentAndSafeWhenNeverStarted(t *testing.T) {
	p := newTestProber(t, healthConfig("http://a.invalid", "http://b.invalid"))
	p.Stop()

	p.Start(context.Background())
	p.Stop()
	p.Stop()
}

func TestFanOut_SkipsUnhealthyBackend(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))
	markDown(t, srv.health, "b")

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if _, asked := b.params(nodesPath); asked {
		t.Fatal("an unhealthy backend was still queried")
	}
	if _, asked := a.params(nodesPath); !asked {
		t.Fatal("the healthy backend was not queried")
	}
	if !strings.Contains(rec.Body.String(), "h1") || strings.Contains(rec.Body.String(), "h2") {
		t.Fatalf("unexpected merged body: %s", rec.Body.String())
	}
	if got := rec.Header().Get(backendsHeader); got != "1/2" {
		t.Errorf("%s = %q, want 1/2", backendsHeader, got)
	}
}

func TestFanOut_RecoveredBackendIsQueriedAgain(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))
	markDown(t, srv.health, "b")
	doGet(t, srv.Handler(), nodesPath, "")

	for i := 0; i < srv.health.successes; i++ {
		srv.health.record("b", nil)
	}
	if !srv.health.healthy("b") {
		t.Fatal("b should be healthy again")
	}

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if _, asked := b.params(nodesPath); !asked {
		t.Fatal("a recovered backend was not queried again")
	}
	if !strings.Contains(rec.Body.String(), "h2") {
		t.Fatalf("recovered backend's records missing: %s", rec.Body.String())
	}
	if got := rec.Header().Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, got)
	}
}

// Failing open matters more than skipping dead backends: a broken prober must
// not black-hole every query.
func TestFanOut_FailsOpenWhenEveryBackendIsUnhealthy(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))
	markDown(t, srv.health, "a")
	markDown(t, srv.health, "b")

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if _, asked := a.params(nodesPath); !asked {
		t.Error("a was not queried despite being the only kind of backend left")
	}
	if _, asked := b.params(nodesPath); !asked {
		t.Error("b was not queried despite being the only kind of backend left")
	}
	body := rec.Body.String()
	if !strings.Contains(body, "h1") || !strings.Contains(body, "h2") {
		t.Fatalf("fail-open should have merged both backends: %s", body)
	}
}

func TestFanOut_UnprobedBackendsAreQueried(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))

	doGet(t, srv.Handler(), nodesPath, "")
	if _, asked := a.params(nodesPath); !asked {
		t.Error("a was skipped before its first probe")
	}
	if _, asked := b.params(nodesPath); !asked {
		t.Error("b was skipped before its first probe")
	}
}

func TestHealthProbe_DisabledIsANoOp(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	cfg := healthConfig(a.srv.URL, b.srv.URL)
	cfg.HealthProbe = false
	srv := newTestServer(cfg)

	if srv.health != nil {
		t.Fatal("probing is disabled but a prober was built")
	}
	srv.StartProbes(context.Background())
	srv.StopProbes()

	rec := doGet(t, srv.Handler(), nodesPath, "")
	for name, fb := range map[string]*fakeBackend{"a": a, "b": b} {
		if _, asked := fb.params(nodesPath); !asked {
			t.Errorf("backend %s was not queried", name)
		}
		if _, probed := fb.params(defaultHealthProbePath); probed {
			t.Errorf("backend %s was probed with probing disabled", name)
		}
	}
	body := rec.Body.String()
	if !strings.Contains(body, "h1") || !strings.Contains(body, "h2") {
		t.Fatalf("unexpected merged body: %s", body)
	}
}

func TestHealthz_ReportsProbeState(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))
	srv.health.record("a", nil)
	markDown(t, srv.health, "b")

	rec := doGet(t, srv.Handler(), "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var hr healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}

	if got := hr.Backends["a"].State; got != stateHealthy {
		t.Errorf("a state = %q, want %q", got, stateHealthy)
	}
	down := hr.Backends["b"]
	if down.State != stateUnhealthy {
		t.Errorf("b state = %q, want %q", down.State, stateUnhealthy)
	}
	if down.Failures != srv.health.failures {
		t.Errorf("b consecutive_failures = %d, want %d", down.Failures, srv.health.failures)
	}
	if down.LastError == "" || down.LastProbe == "" {
		t.Errorf("b is missing last_error/last_probe: %+v", down)
	}
	// A backend the prober has down is still reported, and still reachable.
	if down.Reachable != "ok" {
		t.Errorf("b reachable = %q, want ok", down.Reachable)
	}
}

func TestHealthz_ReportsUnmonitoredWhenProbingIsDisabled(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	cfg := healthConfig(a.srv.URL, b.srv.URL)
	cfg.HealthProbe = false
	srv := newTestServer(cfg)

	rec := doGet(t, srv.Handler(), "/healthz", "")
	var hr healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}
	if got := hr.Backends["a"].State; got != stateUnmonitored {
		t.Errorf("a state = %q, want %q", got, stateUnmonitored)
	}
}

func TestHealthz_ReportsPartialRounds(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	b.fail = true
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))

	if q := srv.queryHealth(); q.Partial {
		t.Fatalf("no query has run yet, but partial is set: %+v", q)
	}

	doGet(t, srv.Handler(), nodesPath, "")
	var hr healthReport
	rec := doGet(t, srv.Handler(), "/healthz", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}
	if !hr.Query.Partial || hr.Query.Contributed != 1 || hr.Query.Configured != 2 {
		t.Fatalf("partial round not reported: %+v", hr.Query)
	}
	if hr.Query.PartialRounds != 1 || hr.Query.LastPartial == "" {
		t.Fatalf("partial round not counted: %+v", hr.Query)
	}

	b.fail = false
	doGet(t, srv.Handler(), nodesPath, "")
	rec = doGet(t, srv.Handler(), "/healthz", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}
	if hr.Query.Partial || hr.Query.Contributed != 2 {
		t.Fatalf("full round still reported as partial: %+v", hr.Query)
	}
}

func TestBackendsHeader_CountsContributors(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))

	if got := doGet(t, srv.Handler(), nodesPath, "").Header().Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, got)
	}

	b.fail = true
	if got := doGet(t, srv.Handler(), nodesPath, "").Header().Get(backendsHeader); got != "1/2" {
		t.Errorf("%s = %q, want 1/2", backendsHeader, got)
	}
}

// A reply that refuses the probe request says nothing about the backend, so the
// backend must keep serving queries instead of being stranded out of the pool.
func TestProber_RejectedProbeKeepsBackendInService(t *testing.T) {
	codes := []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone,
		http.StatusTooManyRequests, http.StatusNotImplemented,
	}
	for _, code := range codes {
		sb := newStatusBackend(t)
		sb.reply(code, "nope")
		p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))

		err := p.probe(t.Context(), Backend{Name: "a", URL: sb.srv.URL})
		if classifyProbe(err) != outcomeRejected {
			t.Fatalf("HTTP %d classified as %v, want a rejected probe (err %v)", code, classifyProbe(err), err)
		}
		for i := 0; i < p.failures+3; i++ {
			p.record("a", err)
			if !p.healthy("a") {
				t.Fatalf("HTTP %d excluded backend a after %d probes", code, i+1)
			}
		}
		snap := p.snapshot()["a"]
		if got := snap.stateName(); got != stateProbeUnsupported {
			t.Errorf("HTTP %d state = %q, want %q", code, got, stateProbeUnsupported)
		}
		if !strings.Contains(snap.LastErr, defaultHealthProbePath) {
			t.Errorf("HTTP %d last_error %q does not name the probe path", code, snap.LastErr)
		}
	}
}

// 503 is trapperkeeper's answer when its services are not nominal, so it stays a
// real failure; so does anything that never got an HTTP reply at all.
func TestProber_ServerErrorsAndTransportFailuresStillMarkDown(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		sb := newStatusBackend(t)
		sb.reply(code, degradedStatusBody("error"))
		p := newTestProber(t, healthConfig(sb.srv.URL, "http://b.invalid"))

		err := p.probe(t.Context(), Backend{Name: "a", URL: sb.srv.URL})
		if classifyProbe(err) != outcomeFailure {
			t.Fatalf("HTTP %d classified as %v, want a real failure", code, classifyProbe(err))
		}
		for i := 0; i < p.failures; i++ {
			p.record("a", err)
		}
		if p.healthy("a") {
			t.Errorf("HTTP %d left backend a in service", code)
		}
		if got := p.snapshot()["a"].stateName(); got != stateUnhealthy {
			t.Errorf("HTTP %d state = %q, want %q", code, got, stateUnhealthy)
		}
	}

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	p := newTestProber(t, healthConfig(deadURL, "http://b.invalid"))
	err := p.probe(t.Context(), Backend{Name: "a", URL: deadURL})
	if classifyProbe(err) != outcomeFailure {
		t.Fatalf("a refused connection classified as %v, want a real failure (err %v)", classifyProbe(err), err)
	}
	for i := 0; i < p.failures; i++ {
		p.record("a", err)
	}
	if p.healthy("a") {
		t.Error("a refused connection left the backend in service")
	}
}

func TestProber_RejectedRunAndFailureRunDoNotAddUp(t *testing.T) {
	p := newTestProber(t, healthConfig("http://a.invalid", "http://b.invalid"))
	markUnsupported(t, p, "a")
	for i := 0; i < 20; i++ {
		p.record("a", &probeRejectedError{status: http.StatusNotFound, path: p.path})
	}

	// One real failure on top of a long rejected run must not trip the threshold.
	p.record("a", errors.New("boom"))
	if !p.healthy("a") {
		t.Fatal("a single real failure after a rejected run marked the backend down")
	}
	for i := 1; i < p.failures; i++ {
		p.record("a", errors.New("boom"))
	}
	if p.healthy("a") {
		t.Fatal("a full run of real failures should mark the backend down")
	}
	if got := p.snapshot()["a"].stateName(); got != stateUnhealthy {
		t.Errorf("state = %q, want %q", got, stateUnhealthy)
	}
}

func TestProber_RecoversFromProbeUnsupported(t *testing.T) {
	p := newTestProber(t, healthConfig("http://a.invalid", "http://b.invalid"))
	markUnsupported(t, p, "a")

	for i := 0; i < p.successes; i++ {
		p.record("a", nil)
	}
	if !p.healthy("a") {
		t.Fatal("backend left the pool while recovering")
	}
	if got := p.snapshot()["a"].stateName(); got != stateHealthy {
		t.Fatalf("state = %q, want %q", got, stateHealthy)
	}
}

// A rejected probe re-admits a backend the prober had already marked down: the
// evidence that condemned it has been replaced by no evidence at all.
func TestProber_RejectedProbeReadmitsADownBackend(t *testing.T) {
	p := newTestProber(t, healthConfig("http://a.invalid", "http://b.invalid"))
	markDown(t, p, "a")
	markUnsupported(t, p, "a")
}

func TestProber_LogsProbeRejectionOncePerTransition(t *testing.T) {
	var buf strings.Builder
	p := newProber(healthConfig("http://a.invalid", "http://b.invalid"), log.New(&buf, "", 0))
	if p == nil {
		t.Fatal("newProber returned nil for an enabled config")
	}

	rejected := &probeRejectedError{status: http.StatusNotFound, path: p.path}
	for i := 0; i < 50; i++ {
		p.record("a", rejected)
	}
	if n := strings.Count(buf.String(), "rejected the health probe"); n != 1 {
		t.Fatalf("logged the misconfiguration %d times, want once:\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), `backend "a"`) || !strings.Contains(buf.String(), defaultHealthProbePath) {
		t.Errorf("log line names neither the backend nor the probe path: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "404") {
		t.Errorf("log line does not name the status: %s", buf.String())
	}

	// Recovery logs its own single line, and a second rejected run logs again.
	for i := 0; i < p.successes+5; i++ {
		p.record("a", nil)
	}
	if n := strings.Count(buf.String(), "usable again"); n != 1 {
		t.Fatalf("logged recovery %d times, want once:\n%s", n, buf.String())
	}
	for i := 0; i < 50; i++ {
		p.record("a", rejected)
	}
	if n := strings.Count(buf.String(), "rejected the health probe"); n != 2 {
		t.Fatalf("second rejected run logged %d times in total, want 2:\n%s", n, buf.String())
	}
}

// The regression this guards: a backend whose probe path is wrong for it alone
// still answers queries perfectly, and must keep contributing records.
func TestFanOut_ProbeUnsupportedBackendStillContributes(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))
	markUnsupported(t, srv.health, "b")

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if _, asked := b.params(nodesPath); !asked {
		t.Fatal("a backend with an unusable probe path was not queried")
	}
	body := rec.Body.String()
	if !strings.Contains(body, "h1") || !strings.Contains(body, "h2") {
		t.Fatalf("both backends should have contributed: %s", body)
	}
	if got := rec.Header().Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, got)
	}
}

func TestHealthz_ReportsProbeUnsupported(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(healthConfig(a.srv.URL, b.srv.URL))
	srv.health.record("a", nil)
	markUnsupported(t, srv.health, "b")

	rec := doGet(t, srv.Handler(), "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var hr healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}
	if got := hr.Backends["a"].State; got != stateHealthy {
		t.Errorf("a state = %q, want %q", got, stateHealthy)
	}
	// Distinct from both "verified healthy" and "excluded".
	unsupported := hr.Backends["b"]
	if unsupported.State != stateProbeUnsupported {
		t.Errorf("b state = %q, want %q", unsupported.State, stateProbeUnsupported)
	}
	if unsupported.Reachable != "ok" {
		t.Errorf("b reachable = %q, want ok", unsupported.Reachable)
	}
	if unsupported.LastError == "" || unsupported.LastProbe == "" {
		t.Errorf("b is missing last_error/last_probe: %+v", unsupported)
	}
}

func TestStatusBodyHealthy_IgnoresBodiesWithoutStates(t *testing.T) {
	for _, body := range []string{`{}`, `[]`, `null`, `{"svc":{}}`, `not json`} {
		if err := statusBodyHealthy([]byte(body)); err != nil {
			t.Errorf("body %q rejected: %v", body, err)
		}
	}
}
