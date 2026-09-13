//go:build e2e

package main

import (
	"context"
	"net/url"
	"strings"
	"testing"
)

// Every backend gets the same query, so a query none of them can answer is the
// client's mistake rather than an outage. openvoxdb explains what is wrong with
// it; pdbmux has to hand that explanation and its status back instead of a 502
// saying the estate is down.
func TestRejectedQuerySurfacesUpstreamStatus(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, path, q string }{
		{"merged", nodesPath, `["=","bogus","x"]`},
		{"combined", nodesPath, `["extract",[["function","count"]],["=","bogus","x"]]`},
		{"union", reportsPath, `["=","bogus","x"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := query(tc.q)
			status, upstream := h.a.queryRaw(ctx, t, tc.path, params)
			if !replayableStatus(status) || status >= 500 {
				t.Fatalf("backend %s answered HTTP %d for %s, which is not the client error this test needs: %s",
					h.a.name, status, tc.q, upstream)
			}
			if other, _ := h.b.queryRaw(ctx, t, tc.path, params); other != status {
				t.Fatalf("backends answered %d and %d, so the rejection is not unanimous", status, other)
			}

			_, before := healthz(t)
			resp := rawGet(t, tc.path, params)
			if resp.status != status {
				t.Fatalf("pdbmux answered HTTP %d, want the upstream %d: %s", resp.status, status, resp.body)
			}
			if got, want := strings.TrimSpace(string(resp.body)), strings.TrimSpace(string(upstream)); got != want {
				t.Errorf("pdbmux body = %q, want openvoxdb's own explanation %q", got, want)
			}
			for _, b := range []*backend{h.a, h.b} {
				if strings.Contains(string(resp.body), b.url) {
					t.Errorf("replayed body names backend %s: %q", b.name, resp.body)
				}
			}

			// A refused query is not degraded service, so it must leave the merged
			// fan-out's own health counters where it found them.
			_, after := healthz(t)
			if after.Query.PartialRounds != before.Query.PartialRounds || after.Query.Partial {
				t.Errorf("query health = %+v, want %+v unchanged by a refused query", after.Query, before.Query)
			}
			if after.Status != "ok" {
				t.Errorf("/healthz = %q after a refused query, want ok", after.Status)
			}
		})
	}
}

// openvoxdb answers the same malformed clause with 400 on /nodes and 500 on
// /facts — engine.clj's rewrite-fact-query runs an unguarded nth only for the
// facts entity. Both are the backends' real answer, so both have to reach the
// client rather than one of them becoming a 502.
func TestUnanimousServerErrorSurfacesUpstreamStatus(t *testing.T) {
	ctx := context.Background()
	params := query(`["=","name"]`)

	status, upstream := h.a.queryRaw(ctx, t, factsPath, params)
	if status < 500 {
		t.Skipf("backend %s answered HTTP %d for the arity bug this test needs", h.a.name, status)
	}
	if other, _ := h.b.queryRaw(ctx, t, factsPath, params); other != status {
		t.Fatalf("backends answered %d and %d, so the failure is not unanimous", status, other)
	}

	resp := rawGet(t, factsPath, params)
	if resp.status != status {
		t.Fatalf("pdbmux answered HTTP %d, want the upstream %d: %s", resp.status, status, resp.body)
	}
	if got, want := strings.TrimSpace(string(resp.body)), strings.TrimSpace(string(upstream)); want != "" && got != want {
		t.Errorf("pdbmux body = %q, want openvoxdb's own explanation %q", got, want)
	}
	for _, b := range []*backend{h.a, h.b} {
		if strings.Contains(string(resp.body), b.url) {
			t.Errorf("replayed body names backend %s: %q", b.name, resp.body)
		}
	}
}

// /metrics and /pdb/meta had their own copy of the gateway error, so a status
// both backends agreed on never reached the client on those routes.
func TestUnanimousMetricsStatusIsReplayed(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{"/metrics/v1/mbeans", "/metrics/v2/list"} {
		t.Run(path, func(t *testing.T) {
			status, _ := h.a.queryRaw(ctx, t, path, nil)
			if status < 400 {
				t.Skipf("%s answers HTTP %d on this estate, so there is nothing to replay", path, status)
			}
			if other, _ := h.b.queryRaw(ctx, t, path, nil); other != status {
				t.Fatalf("backends answered %d and %d, so the status is not unanimous", status, other)
			}
			if resp := rawGet(t, path, nil); resp.status != status {
				t.Fatalf("pdbmux answered HTTP %d, want the upstream %d: %s", resp.status, status, resp.body)
			}
		})
	}
}

// A client cannot tell pdbmux from a PuppetDB by the content type either.
func TestSuccessContentTypeMatchesUpstream(t *testing.T) {
	ctx := context.Background()
	params := url.Values{"limit": {"1"}}
	_, _ = h.a.queryRaw(ctx, t, nodesPath, params)

	resp := rawGet(t, nodesPath, params)
	if resp.status != 200 {
		t.Fatalf("status %d: %s", resp.status, resp.body)
	}
	if got := resp.header.Get("Content-Type"); got != "application/json;charset=utf-8" {
		t.Errorf("Content-Type = %q, want openvoxdb's own %q", got, "application/json;charset=utf-8")
	}
}
