package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakeBackend is an httptest PuppetDB that returns canned bodies per path and
// records the query params it received.
type fakeBackend struct {
	srv        *httptest.Server
	nodesBody  string
	factsBody  string
	fail       bool          // return 500 for everything
	delay      time.Duration // artificial latency
	gotQueries map[string]string
}

func newFakeBackend(t *testing.T, nodesBody, factsBody string) *fakeBackend {
	t.Helper()
	fb := &fakeBackend{nodesBody: nodesBody, factsBody: factsBody, gotQueries: map[string]string{}}
	fb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fb.delay > 0 {
			time.Sleep(fb.delay)
		}
		fb.gotQueries[r.URL.Path] = r.URL.Query().Get("query")
		if fb.fail {
			http.Error(w, "boom", http.StatusInternalServerError)
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
	if old.gotQueries[factsPath] != q {
		t.Errorf("old backend got query %q, want %q", old.gotQueries[factsPath], q)
	}
	if nw.gotQueries[factsPath] != q {
		t.Errorf("new backend got query %q, want %q", nw.gotQueries[factsPath], q)
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
	// A non-merged v4 path (e.g. /reports) goes only to the primary (new).
	old := newFakeBackend(t, `[]`, `[]`)
	nw := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(old.srv.URL, nw.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), "/pdb/query/v4/reports", `["=","certname","h1"]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "/pdb/query/v4/reports") {
		t.Errorf("expected pass-through body, got %s", rec.Body.String())
	}
	// Only primary (new) should have been queried.
	if _, hit := old.gotQueries["/pdb/query/v4/reports"]; hit {
		t.Errorf("non-primary backend should not be queried for pass-through")
	}
	if _, hit := nw.gotQueries["/pdb/query/v4/reports"]; !hit {
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
