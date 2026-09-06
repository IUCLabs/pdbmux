//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// fatalf is the part of *testing.T the harness helpers need, so the same helpers
// can run from TestMain, where there is no real *testing.T.
type fatalf interface {
	Helper()
	Fatalf(format string, args ...any)
}

// bootT lets the harness helpers abort setup in TestMain. A panic unwinds
// through the cleanup defers, which a t.Fatalf's Goexit would not.
type bootT struct{}

func (bootT) Helper() {}

func (bootT) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

// harness is the whole system under test: two real openvoxdb backends and one
// pdbmux in front of them, shared by every test in the suite because standing
// the backends up costs the better part of a minute.
type harness struct {
	a, b        *backend
	mux         *httptest.Server
	port        int
	puppetboard string
}

var h *harness

func TestMain(m *testing.M) {
	os.Exit(runSuite(m))
}

func runSuite(m *testing.M) (code int) {
	// Registered first so it runs last, after every cleanup below.
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "e2e harness setup failed:", r)
			code = 1
		}
	}()

	// Ryuk cannot start in some sandboxed environments; the harness terminates
	// everything it created itself instead.
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}

	ctx := context.Background()
	bt := bootT{}

	netName, dropNetwork := newNetwork(ctx, bt)
	defer dropNetwork()

	a := startBackend(ctx, bt, backendAName, netName)
	defer a.terminate(ctx)
	b := startBackend(ctx, bt, backendBName, netName)
	defer b.terminate(ctx)

	loadFixtures(ctx, bt, a, b)

	cfg := DefaultConfig()
	cfg.Backends = []Backend{{Name: a.name, URL: a.url}, {Name: b.name, URL: b.url}}
	// The cache would answer later requests from a snapshot taken before a
	// backend was killed, hiding exactly the behaviour under test.
	cfg.FactsTTL = 0
	cfg.FreshnessTTL = time.Second
	// A tight probe loop keeps the health test to a few seconds without weakening
	// what it proves: the same debounce thresholds still have to be crossed.
	cfg.HealthProbeInterval = 500 * time.Millisecond
	cfg.HealthProbeTimeout = time.Second
	cfg.HealthProbeFailures = 2
	cfg.HealthProbeSuccesses = 2
	if err := cfg.Validate(); err != nil {
		bt.Fatalf("harness config is invalid: %v", err)
	}

	srv := NewServer(cfg, log.New(os.Stderr, "pdbmux: ", 0))
	srv.StartProbes(ctx)
	defer srv.StopProbes()

	// Bound to all interfaces so the Puppetboard container can reach it through
	// the testcontainers host-access tunnel.
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		bt.Fatalf("listening for pdbmux: %v", err)
	}
	mux := &httptest.Server{Listener: ln, Config: &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}}
	mux.Start()
	defer mux.Close()

	port := ln.Addr().(*net.TCPAddr).Port
	dropTunnel := startHostTunnel(ctx, bt, netName, port)
	defer dropTunnel()
	pbURL, dropPuppetboard := startPuppetboard(ctx, bt, netName, port)
	defer dropPuppetboard()

	h = &harness{a: a, b: b, mux: mux, port: port, puppetboard: pbURL}
	return m.Run()
}

// response is one raw answer from pdbmux, kept whole so tests can assert on the
// headers as well as the records.
type response struct {
	status  int
	header  http.Header
	body    []byte
	request string
}

func (r response) rows(t *testing.T) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal(r.body, &rows); err != nil {
		t.Fatalf("%s: decoding response: %v: %s", r.request, err, r.body)
	}
	return rows
}

// get issues a GET against pdbmux and fails on anything but 200.
func get(t *testing.T, path string, params url.Values) response {
	t.Helper()
	r := rawGet(t, path, params)
	if r.status != http.StatusOK {
		t.Fatalf("%s: want HTTP 200, got %d: %s", r.request, r.status, strings.TrimSpace(string(r.body)))
	}
	return r
}

func rawGet(t *testing.T, path string, params url.Values) response {
	t.Helper()
	target := h.mux.URL + path
	if len(params) > 0 {
		target += "?" + params.Encode()
	}
	resp, err := http.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("GET %s: reading body: %v", target, err)
	}
	return response{status: resp.StatusCode, header: resp.Header, body: body, request: "GET " + target}
}

func query(q string) url.Values { return url.Values{"query": {q}} }

// certnames returns the distinct certnames of a record set, sorted.
func e2eCertnames(rows []map[string]any) []string {
	seen := map[string]bool{}
	for _, row := range rows {
		if cn, ok := row["certname"].(string); ok && cn != "" {
			seen[cn] = true
		}
	}
	return sortedKeys(seen)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// factValue returns the value of one node's fact from a /facts record set.
func factValue(rows []map[string]any, certname, name string) (any, bool) {
	for _, row := range rows {
		if row["certname"] == certname && row["name"] == name {
			return row["value"], true
		}
	}
	return nil, false
}

func nodeRow(t *testing.T, rows []map[string]any, certname string) map[string]any {
	t.Helper()
	for _, row := range rows {
		if row["certname"] == certname {
			return row
		}
	}
	t.Fatalf("no record for %s in %v", certname, e2eCertnames(rows))
	return nil
}

// countOf reads the single ["function","count"] row an aggregate query returns.
func countOf(t *testing.T, rows []map[string]any) int {
	t.Helper()
	if len(rows) != 1 {
		t.Fatalf("want exactly one aggregate row, got %d: %v", len(rows), rows)
	}
	n, ok := rows[0]["count"].(float64)
	if !ok {
		t.Fatalf("aggregate row has no numeric count: %v", rows[0])
	}
	return int(n)
}

func backendCount(ctx context.Context, t *testing.T, b *backend, path, q string) int {
	t.Helper()
	return countOf(t, b.query(ctx, t, path, query(q)))
}

// waitFor polls until cond holds, so tests never sleep for a fixed period.
func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
