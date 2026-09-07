package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBackend is an httptest PuppetDB that returns canned bodies per path and
// records the query params it received.
type fakeBackend struct {
	srv       *httptest.Server
	nodesBody string
	factsBody string
	// bodies holds extra canned responses keyed by path (reports, events, a
	// report's sub-resources). A path under /reports/ that is absent from bodies
	// answers 404, like a PuppetDB that does not hold that report.
	bodies map[string]string
	// totals is the X-Records count advertised per path when the request asks
	// for include_total.
	totals map[string]int
	fail   bool          // return 500 for everything
	delay  time.Duration // artificial latency

	mu        sync.Mutex
	gotParams map[string]url.Values
	// rawPaths records the still-escaped request paths, so tests can assert an
	// MBean name's percent-encoding survived the proxy.
	rawPaths []string
}

func newFakeBackend(t *testing.T, nodesBody, factsBody string) *fakeBackend {
	t.Helper()
	fb := &fakeBackend{
		nodesBody: nodesBody,
		factsBody: factsBody,
		bodies:    map[string]string{},
		totals:    map[string]int{},
		gotParams: map[string]url.Values{},
	}
	fb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fb.delay > 0 {
			time.Sleep(fb.delay)
		}
		fb.mu.Lock()
		fb.gotParams[r.URL.Path] = r.URL.Query()
		fb.rawPaths = append(fb.rawPaths, r.URL.EscapedPath())
		fb.mu.Unlock()
		if fb.fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if body, ok := fb.bodies[r.URL.Path]; ok {
			if n, ok := fb.totals[r.URL.Path]; ok && r.URL.Query().Get("include_total") == "true" {
				w.Header().Set(recordsHeader, strconv.Itoa(n))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, truncate(t, body, r.URL.Query().Get("limit")))
			return
		}
		if strings.HasPrefix(r.URL.Path, reportsPath+"/") {
			http.Error(w, "no report with that hash", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case nodesPath:
			_, _ = io.WriteString(w, fb.nodesBody)
		case factsPath:
			_, _ = io.WriteString(w, fb.factsBody)
		default:
			_, _ = io.WriteString(w, `[{"path":"`+r.URL.Path+`"}]`)
		}
	}))
	t.Cleanup(fb.srv.Close)
	return fb
}

// params returns the query params the backend saw for a path, and whether it was
// asked for that path at all.
func (fb *fakeBackend) params(path string) (url.Values, bool) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	v, ok := fb.gotParams[path]
	return v, ok
}

// sawRawPath reports whether the backend was asked for a path with exactly that
// escaping.
func (fb *fakeBackend) sawRawPath(p string) bool {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return slices.Contains(fb.rawPaths, p)
}

// gotQuery returns the PuppetDB query param the backend saw for a path.
func (fb *fakeBackend) gotQuery(path string) string {
	v, _ := fb.params(path)
	return v.Get("query")
}

// truncate applies an upstream limit param to a canned JSON array body, the way
// a real PuppetDB would, so paging tests exercise the proxy's re-paging.
func truncate(t *testing.T, body, limit string) string {
	t.Helper()
	n, err := strconv.Atoi(limit)
	if err != nil {
		return body
	}
	var raws []json.RawMessage
	if err := json.Unmarshal([]byte(body), &raws); err != nil {
		return body
	}
	if n < len(raws) {
		raws = raws[:n]
	}
	out, err := json.Marshal(raws)
	if err != nil {
		t.Fatalf("re-marshal truncated body: %v", err)
	}
	return string(out)
}

func testConfig(aURL, bURL, merge string) Config {
	return Config{
		Listen:            ":0",
		Backends:          []Backend{{Name: "a", URL: aURL}, {Name: "b", URL: bURL}},
		Merge:             merge,
		Timeout:           2 * time.Second,
		FreshnessTTL:      30 * time.Second,
		SourceFact:        defaultSourceFact,
		SourceFactEnabled: true,
	}
}

func newTestServer(cfg Config) *Server {
	return NewServer(cfg, log.New(io.Discard, "", 0))
}

func doGet(t *testing.T, h http.Handler, path, query string) *httptest.ResponseRecorder {
	t.Helper()
	target := path
	if query != "" {
		target += "?query=" + url.QueryEscape(query)
	}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHandler_NodesMerged(t *testing.T) {
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-10T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, `["=","certname","h1"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got []recordMeta
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 deduped nodes, got %d: %s", len(got), rec.Body.String())
	}
	for _, m := range got {
		if m.Certname == "h1" && m.ReportTimestamp != "2026-07-20T00:00:00Z" {
			t.Errorf("h1 should be the newer record, got %s", m.ReportTimestamp)
		}
	}
}

func TestHandler_QueryPassthrough(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	q := `["=","certname","abc.example.net"]`
	doGet(t, srv.Handler(), factsPath, q)
	if a.gotQuery(factsPath) != q {
		t.Errorf("a backend got query %q, want %q", a.gotQuery(factsPath), q)
	}
	if b.gotQuery(factsPath) != q {
		t.Errorf("b backend got query %q, want %q", b.gotQuery(factsPath), q)
	}
}

func TestHandler_FactsStaticMerge(t *testing.T) {
	// Static merge ignores timestamps: a shared certname resolves to the first
	// backend in configured order that holds it.
	a := newFakeBackend(t, `[]`,
		`[`+fact("h1", "role", "web-a", "")+`,`+fact("h2", "role", "db-a", "")+`]`)
	b := newFakeBackend(t, `[]`,
		`[`+fact("h1", "role", "web-b", "")+`,`+fact("h3", "role", "db-b", "")+`]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "web-a") || strings.Contains(body, "web-b") {
		t.Errorf("h1 should resolve to the first backend holding it: %s", body)
	}
	if !strings.Contains(body, "db-a") || !strings.Contains(body, "db-b") {
		t.Errorf("nodes held by only one backend must all survive: %s", body)
	}
}

func TestHandler_FactsFreshnessMerge(t *testing.T) {
	// Freshness: a holds h1's newer report; b holds h2's newer report.
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`,`+node("h2", "2026-07-01T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-a", "")+`,`+fact("h2", "role", "db-a", "")+`]`)
	b := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-b", "")+`,`+fact("h2", "role", "db-b", "")+`]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	rec := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	// h1 -> a (newer report there); h2 -> b.
	if !strings.Contains(body, "web-a") || strings.Contains(body, "web-b") {
		t.Errorf("h1 should resolve to a: %s", body)
	}
	if !strings.Contains(body, "db-b") || strings.Contains(body, "db-a") {
		t.Errorf("h2 should resolve to b: %s", body)
	}
}

func TestHandler_OneBackendDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving survivor, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "h1") {
		t.Errorf("expected survivor's h1: %s", rec.Body.String())
	}
}

func TestHandler_BothBackendsDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.fail, b.fail = true, true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 when all backends fail, got %d", rec.Code)
	}
}

func TestHandler_PassThroughFirstAnswer(t *testing.T) {
	// A path with no merge rule (e.g. /resources) is served by the first backend
	// that answers; the rest are not asked at all.
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	const path = "/pdb/query/v4/resources"
	rec := doGet(t, srv.Handler(), path, `["=","certname","h1"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), path) {
		t.Errorf("expected pass-through body, got %s", rec.Body.String())
	}
	if _, hit := a.params(path); !hit {
		t.Errorf("first backend should be queried for pass-through")
	}
	if _, hit := b.params(path); hit {
		t.Errorf("later backends should not be queried once one answers")
	}
}

func TestHandler_PassThroughFallsBackToNextBackend(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	const path = "/pdb/query/v4/resources"
	rec := doGet(t, srv.Handler(), path, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected the surviving backend to serve it, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), path) {
		t.Errorf("expected pass-through body, got %s", rec.Body.String())
	}
}

func TestHandler_PassThroughReplaysUpstreamError(t *testing.T) {
	// Every backend rejects it, so PuppetDB's own status reaches the client
	// rather than a synthetic 502.
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.fail, b.fail = true, true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), "/pdb/query/v4/resources", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected the upstream 500 replayed, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "boom") {
		t.Errorf("expected the upstream body, got %s", rec.Body.String())
	}
}

func TestHandler_PostRejected(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))
	req := httptest.NewRequest(http.MethodPost, factsPath, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST, got %d", rec.Code)
	}
}

func TestHandler_Health(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 healthy, got %d: %s", rec.Code, rec.Body.String())
	}
	var hr healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}
	if hr.Status != "ok" || hr.Backends["a"].Reachable != "ok" || hr.Backends["b"].Reachable != "ok" {
		t.Fatalf("unexpected health: %+v", hr)
	}
}

func TestHandler_HealthDegradedAndDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), "/healthz", "")
	var hr healthReport
	_ = json.Unmarshal(rec.Body.Bytes(), &hr)
	if hr.Status != "degraded" {
		t.Errorf("expected degraded, got %s", hr.Status)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("degraded should still be 200, got %d", rec.Code)
	}

	b.fail = true
	rec = doGet(t, srv.Handler(), "/healthz", "")
	_ = json.Unmarshal(rec.Body.Bytes(), &hr)
	if hr.Status != "down" || rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected down/503, got %s/%d", hr.Status, rec.Code)
	}
}

func TestFreshnessCache_Reused(t *testing.T) {
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-a", "")+`]`)
	b := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-b", "")+`]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	// Two facts queries; the freshness /nodes probe should be cached after the
	// first, so query recording only reflects the last observed nodes query but
	// results stay consistent (h1 -> a).
	for i := 0; i < 2; i++ {
		rec := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
		if !strings.Contains(rec.Body.String(), "web-a") {
			t.Fatalf("iteration %d: expected h1->a, got %s", i, rec.Body.String())
		}
	}
}

// doGetParams issues a GET with an arbitrary param set, for the paging/ordering
// params the reports endpoints accept.
func doGetParams(t *testing.T, h http.Handler, path string, params url.Values) *httptest.ResponseRecorder {
	t.Helper()
	target := path
	if len(params) > 0 {
		target += "?" + params.Encode()
	}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// hashes extracts report hashes from a merged response body, in order.
func hashes(t *testing.T, body []byte) []string {
	t.Helper()
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	return hashesOf(t, raws)
}

const receiveDesc = `[{"field":"receive_time","order":"desc"}]`

func TestHandler_ReportsUnioned(t *testing.T) {
	// h1 moved between backends: earlier reports are in a, later ones in b.
	// Both must show up, unlike /facts where one backend wins the node.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[` + report("h1", "r2", "2026-07-10T00:00:00Z") + `,` +
		report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[` + report("h1", "r4", "2026-07-30T00:00:00Z") + `,` +
		report("h1", "r3", "2026-07-20T00:00:00Z") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{
		"query":    {`["=","certname","h1"]`},
		"order_by": {receiveDesc},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := hashes(t, rec.Body.Bytes())
	want := []string{"r4", "r3", "r2", "r1"}
	if !slices.Equal(got, want) {
		t.Errorf("merged reports = %v, want %v (union re-sorted by receive_time desc)", got, want)
	}
}

func TestHandler_ReportsDedupedByHash(t *testing.T) {
	// A node reporting to both PuppetDBs mid-migration stores the same report
	// hash in each; the merged view must show it once.
	dup := report("h1", "r1", "2026-07-01T00:00:00Z")
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[` + dup + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[` + dup + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, nil)
	if got := hashes(t, rec.Body.Bytes()); !slices.Equal(got, []string{"r1"}) {
		t.Errorf("merged reports = %v, want one r1", got)
	}
}

func TestHandler_ReportsPagedAcrossBackends(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[` + report("h1", "r5", "2026-07-05T00:00:00Z") + `,` +
		report("h1", "r3", "2026-07-03T00:00:00Z") + `,` +
		report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[` + report("h1", "r6", "2026-07-06T00:00:00Z") + `,` +
		report("h1", "r4", "2026-07-04T00:00:00Z") + `,` +
		report("h1", "r2", "2026-07-02T00:00:00Z") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{
		"order_by": {receiveDesc},
		"limit":    {"2"},
		"offset":   {"2"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	// Globally-ordered page 2 of the union, not each backend's own page 2.
	if got := hashes(t, rec.Body.Bytes()); !slices.Equal(got, []string{"r4", "r3"}) {
		t.Errorf("page = %v, want [r4 r3]", got)
	}
	// Each backend must be asked for the first offset+limit records so the
	// merged window is fully covered.
	for name, fb := range map[string]*fakeBackend{"a": a, "b": b} {
		p, ok := fb.params(reportsPath)
		if !ok {
			t.Fatalf("%s backend was not queried", name)
		}
		if p.Get("limit") != "4" {
			t.Errorf("%s backend got limit=%q, want 4 (offset+limit)", name, p.Get("limit"))
		}
		if p.Has("offset") {
			t.Errorf("%s backend got offset=%q, want it applied locally instead", name, p.Get("offset"))
		}
	}
}

func TestHandler_ReportsIncludeTotalSummed(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[` + report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	a.totals[reportsPath] = 40
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[` + report("h1", "r2", "2026-07-02T00:00:00Z") + `]`
	b.totals[reportsPath] = 60
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{
		"include_total": {"true"},
		"limit":         {"1"},
	})
	if got := rec.Header().Get(recordsHeader); got != "100" {
		t.Errorf("%s = %q, want 100 (sum of both backends)", recordsHeader, got)
	}
}

func TestHandler_ReportsNoTotalWhenNotRequested(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[]`
	a.totals[reportsPath] = 40
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[]`
	b.totals[reportsPath] = 60
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, nil)
	if got := rec.Header().Get(recordsHeader); got != "" {
		t.Errorf("%s = %q, want it unset without include_total", recordsHeader, got)
	}
}

func TestHandler_ReportsBadPagingParam(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, params := range []url.Values{
		{"limit": {"lots"}},
		{"offset": {"-1"}},
		{"order_by": {"receive_time"}},
	} {
		rec := doGetParams(t, srv.Handler(), reportsPath, params)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%v: expected 400, got %d", params, rec.Code)
		}
	}
}

func TestHandler_EventsUnioned(t *testing.T) {
	// Puppetboard fetches a report's events as /events?query=["=","report",hash],
	// and the report may live in either backend.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventsPath] = `[` + event("h1", "r1", "Package[nginx]") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventsPath] = `[` + event("h1", "r2", "Service[nginx]") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), eventsPath, `["=","certname","h1"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Package[nginx]") || !strings.Contains(body, "Service[nginx]") {
		t.Errorf("expected both backends' events: %s", body)
	}
}

func TestHandler_EventsDedupedByIdentity(t *testing.T) {
	dup := event("h1", "r1", "Package[nginx]")
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventsPath] = `[` + dup + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventsPath] = `[` + dup + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), eventsPath, "")
	var got []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("expected the duplicate event once, got %d: %s", len(got), rec.Body.String())
	}
}

// An events aggregate row is a count, not an event, so the union's
// verbatim-record key would collapse two backends' identical rows into one.
func TestHandler_EventsAggregatesAreCombined(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventsPath] = `[{"count":5}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventsPath] = `[{"count":5}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), eventsPath, `["extract",[["function","count"]]]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{10}) {
		t.Errorf("count = %v, want [10]; the identical rows were deduped instead of added", got)
	}
}

func TestHandler_EventsGroupedAggregatesCombinePerKey(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventsPath] = `[{"status":"success","count":5,"max":7},{"status":"failure","count":1,"max":2}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventsPath] = `[{"status":"success","count":5,"max":3}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	const q = `["extract",[["function","count"],["function","max","line"],"status"],["group_by","status"]]`
	rec := doGet(t, srv.Handler(), eventsPath, q)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{10, 1}) {
		t.Errorf("counts = %v, want [10 1]", got)
	}
	if got := counts(t, rec.Body.Bytes(), "max"); !slices.Equal(got, []float64{7, 2}) {
		t.Errorf("max = %v, want [7 2]; the column was combined by the wrong operation", got)
	}
}

func TestHandler_ReportSubResourceFromHoldingBackend(t *testing.T) {
	// Only a holds report r1, so its logs come from a; an unmerged pass-through
	// to whichever backend answered first could have 404'd.
	const path = reportsPath + "/r1/logs"
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = `[{"level":"notice","message":"from-a"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), path, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "from-a") {
		t.Errorf("expected the holding backend's logs, got %s", rec.Body.String())
	}
}

func TestHandler_ReportSubResourceMissingEverywhere(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), reportsPath+"/nope/events", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when no backend holds the report, got %d", rec.Code)
	}
}

func TestHandler_ReportsOneBackendDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[` + report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{"order_by": {receiveDesc}})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving the survivor, got %d", rec.Code)
	}
	if got := hashes(t, rec.Body.Bytes()); !slices.Equal(got, []string{"r1"}) {
		t.Errorf("merged reports = %v, want [r1]", got)
	}
}

// eventCount builds one /event-counts row for a certname.
func eventCount(certname string, successes, failures, noops, skips int) string {
	return `{"subject_type":"certname","subject":{"title":"` + certname + `"},` +
		`"successes":` + strconv.Itoa(successes) +
		`,"failures":` + strconv.Itoa(failures) +
		`,"noops":` + strconv.Itoa(noops) +
		`,"skips":` + strconv.Itoa(skips) + `}`
}

// counts decodes a numeric column out of a merged aggregate body, in order.
func counts(t *testing.T, body []byte, field string) []float64 {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	out := make([]float64, 0, len(rows))
	for _, r := range rows {
		n, _ := r[field].(float64)
		out = append(out, n)
	}
	return out
}

func TestHandler_EventCountsSummedPerSubject(t *testing.T) {
	// A node reporting to both PuppetDBs has its run counted in each; the
	// merged view is the sum, not two rows.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventCountsPath] = `[` + eventCount("h1", 4, 3, 1, 0) + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventCountsPath] = `[` + eventCount("h1", 2, 1, 0, 0) + `,` + eventCount("h2", 5, 0, 0, 0) + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), eventCountsPath, url.Values{
		"query":        {`["=","certname","h1"]`},
		"summarize_by": {"certname"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	// Rows come out in configured backend order, so h1 leads.
	if got := counts(t, rec.Body.Bytes(), "successes"); !slices.Equal(got, []float64{6, 5}) {
		t.Errorf("successes = %v, want [6 5]", got)
	}
	if got := counts(t, rec.Body.Bytes(), "failures"); !slices.Equal(got, []float64{4, 0}) {
		t.Errorf("failures = %v, want [4 0]", got)
	}
}

func TestHandler_EventCountsDisjointSubjectsPassThrough(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventCountsPath] = `[` + eventCount("h1", 1, 0, 0, 0) + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventCountsPath] = `[` + eventCount("h2", 2, 0, 0, 0) + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), eventCountsPath, url.Values{"summarize_by": {"certname"}})
	if got := counts(t, rec.Body.Bytes(), "successes"); !slices.Equal(got, []float64{1, 2}) {
		t.Errorf("successes = %v, want [1 2] (both nodes, untouched)", got)
	}
	// summarize_by must reach the backends verbatim.
	for name, fb := range map[string]*fakeBackend{"a": a, "b": b} {
		p, _ := fb.params(eventCountsPath)
		if p.Get("summarize_by") != "certname" {
			t.Errorf("%s backend got summarize_by=%q, want certname", name, p.Get("summarize_by"))
		}
	}
}

func TestHandler_EventCountsRecordsIsMergedRowCount(t *testing.T) {
	// Each backend reports one row; they share a subject, so the merged total
	// is one — not the two the backends' own X-Records add up to.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[eventCountsPath] = `[` + eventCount("h1", 1, 0, 0, 0) + `]`
	a.totals[eventCountsPath] = 1
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventCountsPath] = `[` + eventCount("h1", 2, 0, 0, 0) + `]`
	b.totals[eventCountsPath] = 1
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), eventCountsPath, url.Values{
		"summarize_by":  {"certname"},
		"include_total": {"true"},
	})
	if got := rec.Header().Get(recordsHeader); got != "1" {
		t.Errorf("%s = %q, want 1 (merged rows, not 2)", recordsHeader, got)
	}
}

func TestHandler_AggregateEventCountsSummed(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[aggregateEventCountsPath] =
		`[{"successes":2,"failures":1,"noops":0,"skips":3,"total":6,"summarize_by":"certname"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[aggregateEventCountsPath] =
		`[{"successes":5,"failures":4,"noops":1,"skips":0,"total":10,"summarize_by":"certname"}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), aggregateEventCountsPath, url.Values{"summarize_by": {"certname"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one summary object, got %d: %s", len(got), rec.Body.String())
	}
	want := map[string]any{
		"successes": float64(7), "failures": float64(5), "noops": float64(1),
		"skips": float64(3), "total": float64(16), "summarize_by": "certname",
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Errorf("summary = %v, want %v", got[0], want)
	}
}

func TestHandler_AggregateEventCountsNullColumnSurvives(t *testing.T) {
	// PuppetDB returns null totals for an empty result set; summing must not
	// crash or blank out the backend that does have numbers.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[aggregateEventCountsPath] =
		`[{"successes":null,"failures":null,"total":null,"summarize_by":"certname"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[aggregateEventCountsPath] =
		`[{"successes":3,"failures":0,"total":3,"summarize_by":"certname"}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), aggregateEventCountsPath, url.Values{"summarize_by": {"certname"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "total"); !slices.Equal(got, []float64{3}) {
		t.Errorf("total = %v, want [3]", got)
	}
}

const statusCountQuery = `["extract",[["function","count"],"status"],["~","certname",".*"],["group_by","status"]]`

func TestHandler_ReportsAggregateSummed(t *testing.T) {
	// Puppetboard's daily-reports chart: each backend counts only its own
	// reports, so the merged chart needs the per-status sums.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[{"count":4,"status":"changed"},{"count":2,"status":"failed"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[{"count":3,"status":"changed"},{"count":9,"status":"unchanged"}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{"query": {statusCountQuery}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	byStatus := map[string]float64{}
	for _, row := range got {
		s, _ := row["status"].(string)
		n, _ := row["count"].(float64)
		byStatus[s] = n
	}
	want := map[string]float64{"changed": 7, "failed": 2, "unchanged": 9}
	if !reflect.DeepEqual(byStatus, want) {
		t.Errorf("counts = %v, want %v", byStatus, want)
	}
}

// The backends never see the client's avg: they are asked for the sum and count
// it decomposes into, and the weighted average is computed from those.
func TestHandler_AvgIsRewrittenUpstreamAndWeighted(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"sum":10,"count":1}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"sum":60,"count":3}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), resourcesPath, url.Values{
		"query": {`["extract",[["function","avg","line"]]]`},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	const wantUpstream = `["extract",[["function","sum","line"],["function","count","line"]]]`
	for _, fb := range []*fakeBackend{a, b} {
		if got := fb.gotQuery(resourcesPath); got != wantUpstream {
			t.Errorf("backend query = %s, want %s", got, wantUpstream)
		}
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"avg": float64(17.5)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %v, want %v (70/4, not the 15 an average of averages gives)", got, want)
	}
}

// order_by names a column the rewritten query no longer projects, so it is
// dropped upstream and applied to the merged rows here instead.
func TestHandler_AvgOrderByIsDroppedUpstream(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"sum":10,"count":1,"type":"File"},{"sum":8,"count":2,"type":"Stage"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"sum":60,"count":3,"type":"File"}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), resourcesPath, url.Values{
		"query":    {`["extract",[["function","avg","line"],"type"],["group_by","type"]]`},
		"order_by": {`[{"field":"avg","order":"desc"}]`},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if params, _ := a.params(resourcesPath); params.Get("order_by") != "" {
		t.Errorf("backend got order_by %q, want it dropped", params.Get("order_by"))
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{
		{"avg": float64(17.5), "type": "File"},
		{"avg": float64(4), "type": "Stage"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %v, want %v sorted by avg descending", got, want)
	}
}

func TestHandler_UnmergeableAggregateIsRefused(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, path := range []string{nodesPath, factsPath, reportsPath, resourcesPath, factsPath + "/uptime"} {
		rec := doGetParams(t, srv.Handler(), path, url.Values{
			"query": {`["extract",[["function","avg","line"],["function","count"]]]`},
		})
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400: %s", path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "avg") {
			t.Errorf("%s: body %q does not name the limitation", path, rec.Body.String())
		}
	}
	if _, asked := a.params(nodesPath); asked {
		t.Error("a refused query was still fanned out")
	}
}

// max used to be folded by addition, returning a number no backend held.
func TestHandler_MaxIsNotSummed(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"max":20,"min":10}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"max":50,"min":30}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), resourcesPath, url.Values{
		"query": {`["extract",[["function","max","line"],["function","min","line"]]]`},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"max": float64(50), "min": float64(10)}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %v, want %v", got, want)
	}
}

func TestHandler_ReportsAggregateRecordsIsMergedRowCount(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[{"count":4,"status":"changed"}]`
	a.totals[reportsPath] = 1
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[{"count":3,"status":"changed"}]`
	b.totals[reportsPath] = 1
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{
		"query":         {statusCountQuery},
		"include_total": {"true"},
	})
	if got := rec.Header().Get(recordsHeader); got != "1" {
		t.Errorf("%s = %q, want 1 (one merged status row)", recordsHeader, got)
	}
}

func TestHandler_ReportsNonAggregateStillUnioned(t *testing.T) {
	// An extract with no function is a projection of real reports, so the
	// union — not a sum — is still the right merge.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[reportsPath] = `[` + report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[reportsPath] = `[` + report("h1", "r2", "2026-07-02T00:00:00Z") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{
		"query":    {`["extract",["hash","certname"],["=","certname","h1"]]`},
		"order_by": {receiveDesc},
	})
	if got := hashes(t, rec.Body.Bytes()); !slices.Equal(got, []string{"r2", "r1"}) {
		t.Errorf("merged reports = %v, want [r2 r1]", got)
	}
}

func TestHandler_EventCountsOneBackendDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[eventCountsPath] = `[` + eventCount("h1", 2, 0, 0, 0) + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), eventCountsPath, url.Values{"summarize_by": {"certname"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving the survivor, got %d", rec.Code)
	}
	if got := counts(t, rec.Body.Bytes(), "successes"); !slices.Equal(got, []float64{2}) {
		t.Errorf("successes = %v, want [2]", got)
	}
}

func TestHandler_EventCountsBadPagingParam(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), eventCountsPath, url.Values{"limit": {"lots"}})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for a malformed limit, got %d", rec.Code)
	}
}

// What Puppetboard's landing page sends when DEFAULT_ENVIRONMENT names a real
// environment: an extract/count with no group_by, so every backend returns one
// anonymous row.
const nodeCountQuery = `["extract",[["function","count"]],["and",["=","catalog_environment","production"]]]`

func TestHandler_NodesAggregateSummed(t *testing.T) {
	// A count row has no certname, so the certname-keyed merge would have
	// collapsed both backends' counts into one backend's number.
	a := newFakeBackend(t, `[{"count":90}]`, `[]`)
	b := newFakeBackend(t, `[{"count":53}]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, nodeCountQuery)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{143}) {
		t.Errorf("count = %v, want [143]", got)
	}
}

func TestHandler_NodesAggregateGroupedSummed(t *testing.T) {
	a := newFakeBackend(t, `[{"count":4,"catalog_environment":"production"},{"count":1,"catalog_environment":"dev"}]`, `[]`)
	b := newFakeBackend(t, `[{"count":3,"catalog_environment":"production"}]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath,
		`["extract",[["function","count"],"catalog_environment"],["~","certname",".*"],["group_by","catalog_environment"]]`)
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	byEnv := map[string]float64{}
	for _, row := range got {
		e, _ := row["catalog_environment"].(string)
		n, _ := row["count"].(float64)
		byEnv[e] = n
	}
	want := map[string]float64{"production": 7, "dev": 1}
	if !reflect.DeepEqual(byEnv, want) {
		t.Errorf("counts = %v, want %v", byEnv, want)
	}
}

func TestHandler_NodesNonAggregateStillMergedByCertname(t *testing.T) {
	// Regression: routing aggregates to the summing path must not divert plain
	// queries, including an extract projection that carries no function column.
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-10T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, q := range []string{
		`["=","certname","h1"]`,
		`["extract",["certname","report_timestamp"],["~","certname",".*"]]`,
	} {
		rec := doGet(t, srv.Handler(), nodesPath, q)
		var got []recordMeta
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("query %s: %v", q, err)
		}
		if len(got) != 2 {
			t.Fatalf("query %s: expected 2 deduped nodes, got %d: %s", q, len(got), rec.Body.String())
		}
		for _, m := range got {
			if m.Certname == "h1" && m.ReportTimestamp != "2026-07-20T00:00:00Z" {
				t.Errorf("query %s: h1 should be the newer record, got %s", q, m.ReportTimestamp)
			}
		}
	}
}

func TestHandler_FactsAggregateSummed(t *testing.T) {
	// A count row carries no certname, so the per-certname fact merge would have
	// kept whichever backend owned the empty-certname bucket.
	a := newFakeBackend(t, `[]`, `[{"count":7}]`)
	b := newFakeBackend(t, `[]`, `[{"count":10}]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, `["extract",[["function","count"]]]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{17}) {
		t.Errorf("count = %v, want [17]", got)
	}
	if got := rec.Header().Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, got)
	}
	if _, ok := b.params(factsPath); !ok {
		t.Error("second backend was never asked for the fact count")
	}
}

func TestHandler_FactsAggregateGroupedSummed(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[{"count":4,"name":"osfamily"},{"count":1,"name":"only_a"}]`)
	b := newFakeBackend(t, `[]`, `[{"count":3,"name":"osfamily"}]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath,
		`["extract",[["function","count"],"name"],["~","certname",".*"],["group_by","name"]]`)
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	byName := map[string]float64{}
	for _, row := range got {
		n, _ := row["name"].(string)
		byName[n], _ = row["count"].(float64)
	}
	want := map[string]float64{"osfamily": 7, "only_a": 1}
	if !reflect.DeepEqual(byName, want) {
		t.Errorf("counts = %v, want %v", byName, want)
	}
}

// include_total on a summed response reports merged rows, not the backends' own totals.
func TestHandler_FactsAggregateRecordsIsMergedRowCount(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[{"count":4,"name":"osfamily"}]`)
	a.totals[factsPath] = 1
	b := newFakeBackend(t, `[]`, `[{"count":3,"name":"osfamily"}]`)
	b.totals[factsPath] = 1
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), factsPath, url.Values{
		"query":         {`["extract",[["function","count"],"name"],["group_by","name"]]`},
		"include_total": {"true"},
	})
	if got := rec.Header().Get(recordsHeader); got != "1" {
		t.Errorf("%s = %q, want 1", recordsHeader, got)
	}
}

// aggregatePagingBackends hold group counts a per-backend limit would truncate
// to the wrong answer: backend a's own first row is Exec, so a limit pushed
// upstream drops the File rows that together make File the real top group.
func aggregatePagingBackends(t *testing.T) (*fakeBackend, *fakeBackend) {
	t.Helper()
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"count":6,"type":"Exec"},{"count":5,"type":"File"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"count":4,"type":"File"}]`
	return a, b
}

const aggregateCountByType = `["extract",[["function","count","certname"],"type"],["group_by","type"]]`

// A group truncated away on one backend would fold to a wrong total, so the
// whole aggregate is fetched and the window cut after the fold.
func TestHandler_ResourcesAggregateLimitIsAppliedAfterTheFold(t *testing.T) {
	a, b := aggregatePagingBackends(t)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), resourcesPath, url.Values{
		"query":    {aggregateCountByType},
		"order_by": {`[{"field":"count","order":"desc"}]`},
		"limit":    {"1"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"count": float64(9), "type": "File"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("top group = %v, want %v", got, want)
	}
	for name, fb := range map[string]*fakeBackend{"a": a, "b": b} {
		p, ok := fb.params(resourcesPath)
		if !ok {
			t.Fatalf("%s backend was not queried", name)
		}
		if p.Has("limit") || p.Has("offset") {
			t.Errorf("%s backend got limit=%q offset=%q, want both applied locally", name, p.Get("limit"), p.Get("offset"))
		}
	}
}

// The merged row count is the whole aggregate's, not the paged window's.
func TestHandler_ResourcesAggregateIncludeTotalWithLocalPaging(t *testing.T) {
	a, b := aggregatePagingBackends(t)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), resourcesPath, url.Values{
		"query":         {aggregateCountByType},
		"order_by":      {`[{"field":"count","order":"desc"}]`},
		"limit":         {"1"},
		"offset":        {"1"},
		"include_total": {"true"},
	})
	if got := rec.Header().Get(recordsHeader); got != "2" {
		t.Errorf("%s = %q, want 2 merged groups", recordsHeader, got)
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"count": float64(6), "type": "Exec"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("offset window = %v, want %v", got, want)
	}
}

// Row functions return one row per record rather than per group, so nothing is
// folded and the upstream limit that bounds them still applies.
func TestHandler_ResourcesRowFunctionKeepsUpstreamLimit(t *testing.T) {
	a, b := aggregatePagingBackends(t)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), resourcesPath, url.Values{
		"query": {`["extract",[["function","to_string","line"],"type"]]`},
		"limit": {"1"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	p, ok := a.params(resourcesPath)
	if !ok {
		t.Fatal("backend a was not queried")
	}
	if p.Get("limit") != "1" {
		t.Errorf("backend got limit=%q, want it forwarded", p.Get("limit"))
	}
}

func TestHandler_FactsNonAggregateStillMergedByCertname(t *testing.T) {
	// Regression: routing aggregates to the summing path must not divert plain
	// queries, including an extract projection that carries no function column.
	a := newFakeBackend(t, `[]`, `[`+fact("h1", "role", "web", "")+`]`)
	b := newFakeBackend(t, `[]`, `[`+fact("h2", "role", "db", "")+`]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, q := range []string{
		`["~","certname",".*"]`,
		`["extract",["certname","name","value"],["~","certname",".*"]]`,
	} {
		rec := doGet(t, srv.Handler(), factsPath, q)
		if got := rec.Body.String(); !strings.Contains(got, `"h1"`) || !strings.Contains(got, `"h2"`) {
			t.Errorf("query %s: body = %s, want both backends' facts merged", q, got)
		}
	}
}

func TestHandler_ResourcesAggregateSummed(t *testing.T) {
	// /resources is otherwise an unmerged pass-through, so before this the
	// landing page's resource total was whichever backend answered first.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"count":1000}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"count":234}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), resourcesPath,
		`["extract",[["function","count"]],["=","environment","production"]]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{1234}) {
		t.Errorf("count = %v, want [1234]", got)
	}
}

func TestHandler_ResourcesNonAggregateStillPassesThrough(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"certname":"h1","type":"File"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"certname":"h2","type":"File"}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), resourcesPath, `["=","type","File"]`)
	if got := rec.Body.String(); !strings.Contains(got, `"h1"`) || strings.Contains(got, `"h2"`) {
		t.Errorf("body = %s, want the first backend's response verbatim", got)
	}
}

func TestHandler_ResourcesAggregateAsksEveryBackend(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[resourcesPath] = `[{"count":1}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[resourcesPath] = `[{"count":1}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	doGet(t, srv.Handler(), resourcesPath, `["extract",[["function","count"]],["=","environment","production"]]`)
	if _, ok := b.params(resourcesPath); !ok {
		t.Error("second backend was never asked for the resource count")
	}
}
