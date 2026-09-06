//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func healthz(t *testing.T) (int, healthReport) {
	t.Helper()
	resp := rawGet(t, "/healthz", nil)
	var report healthReport
	if err := json.Unmarshal(resp.body, &report); err != nil {
		t.Fatalf("decoding /healthz: %v: %s", err, resp.body)
	}
	return resp.status, report
}

// A dead backend must be taken out of the fan-out, shown as such on /healthz,
// and put back once it answers again — all without a query ever failing.
func TestBackendDeathAndRecovery(t *testing.T) {
	ctx := context.Background()

	if _, report := healthz(t); report.Status != "ok" {
		t.Fatalf("/healthz is %q before the test starts, want ok: %+v", report.Status, report.Backends)
	}

	restored := false
	h.b.stop(ctx, t)
	// However this test ends, the rest of the suite needs both backends back.
	t.Cleanup(func() {
		if restored {
			return
		}
		h.b.start(ctx, t)
	})

	waitUntil(t, "the prober to mark "+h.b.name+" down", 30*time.Second, func() bool {
		_, report := healthz(t)
		return report.Backends[h.b.name].State == stateUnhealthy
	})

	status, report := healthz(t)
	if status != http.StatusOK {
		t.Errorf("/healthz status = %d with one backend up, want 200", status)
	}
	if report.Status != "degraded" {
		t.Errorf("/healthz reports %q with one backend down, want degraded", report.Status)
	}
	if got := report.Backends[h.a.name]; got.State != stateHealthy || got.Reachable != "ok" {
		t.Errorf("surviving backend %s = %+v, want healthy and reachable", h.a.name, got)
	}
	down := report.Backends[h.b.name]
	if down.Reachable == "ok" {
		t.Errorf("dead backend %s still reports reachable=ok", h.b.name)
	}
	if down.LastError == "" {
		t.Errorf("dead backend %s reports no probe error: %+v", h.b.name, down)
	}

	t.Run("queries are served from the survivor", func(t *testing.T) {
		resp := get(t, nodesPath, nil)
		rows := resp.rows(t)
		if got, want := e2eCertnames(rows), []string{nodeAlpha, nodeShared}; !equalStrings(got, want) {
			t.Fatalf("merged /nodes with %s down = %v, want %v", h.b.name, got, want)
		}
		if got := resp.header.Get(backendsHeader); got != "1/2" {
			t.Errorf("%s = %q with one backend down, want %q", backendsHeader, got, "1/2")
		}

		// The shared node's owner has to fall back to the survivor's copy.
		facts := get(t, factsPath, query(`["=","certname","`+nodeShared+`"]`)).rows(t)
		if got, _ := factValue(facts, nodeShared, "owner"); got != backendAName {
			t.Errorf("owner of %s with %s down = %v, want %q", nodeShared, h.b.name, got, backendAName)
		}
		if got, _ := factValue(facts, nodeShared, defaultSourceFact); got != backendAName {
			t.Errorf("%s for %s with %s down = %v, want %q", defaultSourceFact, nodeShared, h.b.name, got, backendAName)
		}
	})

	t.Run("the query report records the partial fan-out", func(t *testing.T) {
		_, report := healthz(t)
		if !report.Query.Partial {
			t.Errorf("query report is not marked partial after a one-backend fan-out: %+v", report.Query)
		}
		if report.Query.Contributed != 1 || report.Query.Configured != 2 {
			t.Errorf("query report = %d/%d contributors, want 1/2", report.Query.Contributed, report.Query.Configured)
		}
	})

	h.b.start(ctx, t)
	restored = true
	waitUntil(t, "backend "+h.b.name+" to be readmitted", time.Minute, func() bool {
		_, report := healthz(t)
		return report.Backends[h.b.name].State == stateHealthy
	})

	_, report = healthz(t)
	if report.Status != "ok" {
		t.Errorf("/healthz reports %q after recovery, want ok", report.Status)
	}
	// The freshness map is cached, so give it a moment to expire before asserting
	// that the recovered backend owns the shared node again.
	waitUntil(t, "the recovered backend to rejoin the fan-out", 30*time.Second, func() bool {
		resp := rawGet(t, nodesPath, nil)
		return resp.status == http.StatusOK && resp.header.Get(backendsHeader) == "2/2"
	})
	waitUntil(t, nodeShared+" to be attributed to "+h.b.name, 30*time.Second, func() bool {
		rows := get(t, factsPath, query(`["=","certname","`+nodeShared+`"]`)).rows(t)
		got, _ := factValue(rows, nodeShared, "owner")
		return got == backendBName
	})
}

// Every backend is queried for the reachability report, including one the prober
// has taken out of service, so /healthz never hides a backend.
func TestHealthReportsEveryConfiguredBackend(t *testing.T) {
	_, report := healthz(t)
	for _, name := range []string{backendAName, backendBName} {
		if _, ok := report.Backends[name]; !ok {
			t.Errorf("/healthz omits backend %s: %+v", name, report.Backends)
		}
	}
	if report.Query.Configured != 2 {
		t.Errorf("/healthz reports %d configured backends, want 2", report.Query.Configured)
	}
}
