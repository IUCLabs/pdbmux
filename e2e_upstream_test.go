//go:build e2e

package main

import (
	"context"
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
			if !clientShaped(status) {
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
