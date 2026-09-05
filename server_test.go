package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func testConfig(oldURL, newURL, merge string) Config {
	return Config{
		Listen:       ":0",
		Backends:     []Backend{{Name: "old", URL: oldURL}, {Name: "new", URL: newURL}},
		Primary:      "new",
		Merge:        merge,
		Prefer:       "new",
		Timeout:      2 * time.Second,
		FreshnessTTL: 30 * time.Second,
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
	old := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-10T00:00:00Z")+`]`, `[]`)
	nw := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

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
			t.Errorf("h1 should be new's newer record, got %s", m.ReportTimestamp)
		}
	}
}

func TestHandler_QueryPassthrough(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	q := `["=","certname","abc.example.net"]`
	doGet(t, srv.Handler(), factsPath, q)
	if old.gotQuery(factsPath) != q {
		t.Errorf("old backend got query %q, want %q", old.gotQuery(factsPath), q)
	}
	if nw.gotQuery(factsPath) != q {
		t.Errorf("new backend got query %q, want %q", nw.gotQuery(factsPath), q)
	}
}

func TestHandler_FactsStaticMerge(t *testing.T) {
	old := newFakeBackend(t, `[]`,
		`[`+fact("h1", "role", "web-old", "")+`,`+fact("h2", "role", "db-old", "")+`]`)
	nw := newFakeBackend(t, `[]`,
		`[`+fact("h1", "role", "web-new", "")+`]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "web-new") || strings.Contains(body, "web-old") {
		t.Errorf("static prefer=new should keep web-new, drop web-old: %s", body)
	}
	if !strings.Contains(body, "db-old") {
		t.Errorf("h2 only in old should survive: %s", body)
	}
}

func TestHandler_FactsFreshnessMerge(t *testing.T) {
	// Freshness: old holds h1's newer report; new holds h2's newer report.
	old := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`,`+node("h2", "2026-07-01T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-old", "")+`,`+fact("h2", "role", "db-old", "")+`]`)
	nw := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-new", "")+`,`+fact("h2", "role", "db-new", "")+`]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeFreshness))

	rec := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	// h1 -> old (newer report there); h2 -> new.
	if !strings.Contains(body, "web-old") || strings.Contains(body, "web-new") {
		t.Errorf("h1 should resolve to old: %s", body)
	}
	if !strings.Contains(body, "db-new") || strings.Contains(body, "db-old") {
		t.Errorf("h2 should resolve to new: %s", body)
	}
}

func TestHandler_OneBackendDown(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	old.fail = true
	nw := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving survivor, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "h1") {
		t.Errorf("expected survivor's h1: %s", rec.Body.String())
	}
}

func TestHandler_BothBackendsDown(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	old.fail, nw.fail = true, true
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 when all backends fail, got %d", rec.Code)
	}
}

func TestHandler_PassThroughToPrimary(t *testing.T) {
	// A non-merged v4 path (e.g. /resources) goes only to the primary (new).
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	const path = "/pdb/query/v4/resources"
	rec := doGet(t, srv.Handler(), path, `["=","certname","h1"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), path) {
		t.Errorf("expected pass-through body, got %s", rec.Body.String())
	}
	// Only primary (new) should have been queried.
	if _, hit := old.params(path); hit {
		t.Errorf("non-primary backend should not be queried for pass-through")
	}
	if _, hit := nw.params(path); !hit {
		t.Errorf("primary backend should be queried for pass-through")
	}
}

func TestHandler_PostRejected(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))
	req := httptest.NewRequest(http.MethodPost, factsPath, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST, got %d", rec.Code)
	}
}

func TestHandler_Health(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 healthy, got %d: %s", rec.Code, rec.Body.String())
	}
	var hr healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatal(err)
	}
	if hr.Status != "ok" || hr.Backends["old"] != "ok" || hr.Backends["new"] != "ok" {
		t.Fatalf("unexpected health: %+v", hr)
	}
}

func TestHandler_HealthDegradedAndDown(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	old.fail = true
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), "/healthz", "")
	var hr healthReport
	_ = json.Unmarshal(rec.Body.Bytes(), &hr)
	if hr.Status != "degraded" {
		t.Errorf("expected degraded, got %s", hr.Status)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("degraded should still be 200, got %d", rec.Code)
	}

	nw.fail = true
	rec = doGet(t, srv.Handler(), "/healthz", "")
	_ = json.Unmarshal(rec.Body.Bytes(), &hr)
	if hr.Status != "down" || rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected down/503, got %s/%d", hr.Status, rec.Code)
	}
}

func TestFreshnessCache_Reused(t *testing.T) {
	old := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-old", "")+`]`)
	nw := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web-new", "")+`]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeFreshness))

	// Two facts queries; the freshness /nodes probe should be cached after the
	// first, so query recording only reflects the last observed nodes query but
	// results stay consistent (h1 -> old).
	for i := 0; i < 2; i++ {
		rec := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
		if !strings.Contains(rec.Body.String(), "web-old") {
			t.Fatalf("iteration %d: expected h1->old, got %s", i, rec.Body.String())
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
	// h1 migrated: its pre-migration reports are in old, later ones in new.
	// Both must show up, unlike /facts where one backend wins the node.
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[reportsPath] = `[` + report("h1", "r2", "2026-07-10T00:00:00Z") + `,` +
		report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[reportsPath] = `[` + report("h1", "r4", "2026-07-30T00:00:00Z") + `,` +
		report("h1", "r3", "2026-07-20T00:00:00Z") + `]`
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

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
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[reportsPath] = `[` + dup + `]`
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[reportsPath] = `[` + dup + `]`
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, nil)
	if got := hashes(t, rec.Body.Bytes()); !slices.Equal(got, []string{"r1"}) {
		t.Errorf("merged reports = %v, want one r1", got)
	}
}

func TestHandler_ReportsPagedAcrossBackends(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[reportsPath] = `[` + report("h1", "r5", "2026-07-05T00:00:00Z") + `,` +
		report("h1", "r3", "2026-07-03T00:00:00Z") + `,` +
		report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[reportsPath] = `[` + report("h1", "r6", "2026-07-06T00:00:00Z") + `,` +
		report("h1", "r4", "2026-07-04T00:00:00Z") + `,` +
		report("h1", "r2", "2026-07-02T00:00:00Z") + `]`
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

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
	for name, fb := range map[string]*fakeBackend{"old": old, "new": nw} {
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
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[reportsPath] = `[` + report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	old.totals[reportsPath] = 40
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[reportsPath] = `[` + report("h1", "r2", "2026-07-02T00:00:00Z") + `]`
	nw.totals[reportsPath] = 60
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{
		"include_total": {"true"},
		"limit":         {"1"},
	})
	if got := rec.Header().Get(recordsHeader); got != "100" {
		t.Errorf("%s = %q, want 100 (sum of both backends)", recordsHeader, got)
	}
}

func TestHandler_ReportsNoTotalWhenNotRequested(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[reportsPath] = `[]`
	old.totals[reportsPath] = 40
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[reportsPath] = `[]`
	nw.totals[reportsPath] = 60
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, nil)
	if got := rec.Header().Get(recordsHeader); got != "" {
		t.Errorf("%s = %q, want it unset without include_total", recordsHeader, got)
	}
}

func TestHandler_ReportsBadPagingParam(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

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
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[eventsPath] = `[` + event("h1", "r1", "Package[nginx]") + `]`
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[eventsPath] = `[` + event("h1", "r2", "Service[nginx]") + `]`
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

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
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[eventsPath] = `[` + dup + `]`
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[eventsPath] = `[` + dup + `]`
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), eventsPath, "")
	var got []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("expected the duplicate event once, got %d: %s", len(got), rec.Body.String())
	}
}

func TestHandler_ReportSubResourceFromHoldingBackend(t *testing.T) {
	// Only old holds report r1, so its logs must come from old even though new
	// is the primary — a pass-through would have 404'd.
	const path = reportsPath + "/r1/logs"
	old := newFakeBackend(t, `[]`, `[]`)
	old.bodies[path] = `[{"level":"notice","message":"from-old"}]`
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), path, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "from-old") {
		t.Errorf("expected old's logs, got %s", rec.Body.String())
	}
}

func TestHandler_ReportSubResourceMissingEverywhere(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), reportsPath+"/nope/events", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when no backend holds the report, got %d", rec.Code)
	}
}

func TestHandler_ReportsOneBackendDown(t *testing.T) {
	old := newFakeBackend(t, `[]`, `[]`)
	old.fail = true
	nw := newFakeBackend(t, `[]`, `[]`)
	nw.bodies[reportsPath] = `[` + report("h1", "r1", "2026-07-01T00:00:00Z") + `]`
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{"order_by": {receiveDesc}})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving the survivor, got %d", rec.Code)
	}
	if got := hashes(t, rec.Body.Bytes()); !slices.Equal(got, []string{"r1"}) {
		t.Errorf("merged reports = %v, want [r1]", got)
	}
}
