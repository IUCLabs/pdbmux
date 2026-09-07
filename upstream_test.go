package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const badOrderBy = `Unrecognized column 'bogus' specified in :order_by`

func upErr(status int, body string) error {
	return newUpstreamError(status, "text/plain; charset=utf-8", []byte(body))
}

func TestUnanimousClientError_AgreementRule(t *testing.T) {
	refused := errors.New("dial tcp: connection refused")
	for _, tc := range []struct {
		name string
		errs []error
		want int // 0 means "no replay"
	}{
		{"all agree on 400", []error{upErr(400, "bad"), upErr(400, "bad")}, 400},
		{"all agree on 415", []error{upErr(415, "bad media"), upErr(415, "bad media")}, 415},
		{"differing bodies still agree", []error{upErr(400, "one"), upErr(400, "two")}, 400},
		{"disagreeing 4xx", []error{upErr(400, "bad"), upErr(404, "gone")}, 0},
		// Both client-shaped, so only the same-status rule refuses these.
		{"agree on shape, disagree on code", []error{upErr(400, "x"), upErr(415, "y")}, 0},
		{"a majority agrees", []error{upErr(400, "x"), upErr(400, "y"), upErr(422, "z")}, 0},
		{"the first differs", []error{upErr(422, "z"), upErr(400, "x"), upErr(400, "y")}, 0},
		{"4xx with a 5xx", []error{upErr(400, "bad"), upErr(500, "boom")}, 0},
		{"4xx with a transport failure", []error{upErr(400, "bad"), refused}, 0},
		{"all 403", []error{upErr(403, "denied"), upErr(403, "denied")}, 0},
		{"all 404", []error{upErr(404, "gone"), upErr(404, "gone")}, 0},
		{"all 429", []error{upErr(429, "slow down"), upErr(429, "slow down")}, 0},
		{"all 408", []error{upErr(408, "too slow"), upErr(408, "too slow")}, 0},
		{"all 500", []error{upErr(500, "boom"), upErr(500, "boom")}, 0},
		{"all 503", []error{upErr(503, "unavailable"), upErr(503, "unavailable")}, 0},
		{"no backends", nil, 0},
		{"a backend succeeded", []error{upErr(400, "bad"), nil}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := make([]backendResult, len(tc.errs))
			for i, err := range tc.errs {
				results[i] = backendResult{name: fmt.Sprintf("b%d", i), err: err}
			}
			got := unanimousClientError(results)
			switch {
			case tc.want == 0 && got != nil:
				t.Fatalf("want no replay, got HTTP %d", got.status)
			case tc.want != 0 && got == nil:
				t.Fatalf("want HTTP %d replayed, got none", tc.want)
			case tc.want != 0 && got.status != tc.want:
				t.Fatalf("status = %d, want %d", got.status, tc.want)
			}
		})
	}
}

// Two backends can explain the same rejection differently; the reply is the
// first in configured order so a retried query is answered the same way twice.
func TestUnanimousClientError_PicksFirstInConfiguredOrder(t *testing.T) {
	results := []backendResult{
		{name: "a", err: upErr(400, "from a")},
		{name: "b", err: upErr(400, "from b")},
	}
	got := unanimousClientError(results)
	if got == nil || !strings.Contains(string(got.body), "from a") {
		t.Fatalf("body = %q, want the first backend's", got)
	}
}

func TestUpstreamError_BodyIsCapped(t *testing.T) {
	ue := newUpstreamError(400, "text/plain", []byte(strings.Repeat("x", upstreamBodyLimit*2)))
	if len(ue.body) != upstreamBodyLimit {
		t.Fatalf("kept %d bytes, want %d", len(ue.body), upstreamBodyLimit)
	}
}

func TestHandler_MergedAllBackendsRejectReplaysUpstreamStatus(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), nodesPath, url.Values{"order_by": {`[{"field":"certname"}]`}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the upstream 400 replayed: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "bogus") {
		t.Errorf("body = %q, want the upstream explanation", rec.Body.String())
	}
}

// 400 is not the only client-shaped status openvoxdb can answer with, so the
// replay carries whatever status the backends agreed on.
func TestHandler_MergedReplaysNonBadRequestStatus(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusUnsupportedMediaType, "unsupported media type"
	b.reject, b.rejectBody = http.StatusUnsupportedMediaType, "unsupported media type"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", rec.Code)
	}
}

// A unanimous 403 is pdbmux's own credentials being refused, not the client's
// query, so it must not be handed back as the client's fault.
func TestHandler_MergedForbiddenStays502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusForbidden, "certificate not allowed"
	b.reject, b.rejectBody = http.StatusForbidden, "certificate not allowed"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

// Every backend answering 404 is ambiguous between a bad path and a record
// nobody holds, so it stays a gateway error on a merged route.
func TestHandler_MergedNotFoundStays502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusNotFound, "Not Found"
	b.reject, b.rejectBody = http.StatusNotFound, "Not Found"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestHandler_MergedMixedRejectionAndServerErrorStays502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.fail = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when only one backend blamed the query", rec.Code)
	}
}

func TestHandler_MergedMixedRejectionAndUnreachableStays502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	dead := newFakeBackend(t, `[]`, `[]`)
	deadURL := dead.srv.URL
	dead.srv.Close()
	srv := newTestServer(testConfig(a.srv.URL, deadURL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when a backend never answered", rec.Code)
	}
}

func TestHandler_MergedDisagreeingRejectionsStay502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusNotFound, "no such entity"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when backends disagree on the rejection", rec.Code)
	}
}

// A backend that cannot answer a query must not fail one another backend can.
func TestHandler_MergedOneRejectionOneAnswerServesTheAnswer(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the surviving backend's 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "h1") {
		t.Errorf("body = %s, want the survivor's record", rec.Body.String())
	}
}

func TestHandler_CombinedAllBackendsRejectReplaysUpstreamStatus(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), resourcesPath, `["extract",[["function","count"]],["=","environment","production"]]`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the aggregate path to replay 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "bogus") {
		t.Errorf("body = %q, want the upstream explanation", rec.Body.String())
	}
}

func TestHandler_CombinedMixedRejectionAndServerErrorStays502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.fail = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), resourcesPath, `["extract",[["function","count"]],["=","environment","production"]]`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestHandler_UnionAllBackendsRejectReplaysUpstreamStatus(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), reportsPath, url.Values{"order_by": {receiveDesc}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the union path to replay 400: %s", rec.Code, rec.Body.String())
	}
}

// /fact-names orders locally, so its fan-out is the third shape aliveResults serves.
func TestHandler_FactNamesAllBackendsRejectReplaysUpstreamStatus(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), factNamesPath, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// A backend answering 404 means it does not hold the report, which is the whole
// premise of this route, so it must keep producing pdbmux's own 404.
func TestHandler_ReportSubResourceRejectionReplayedButNotItsAbsence(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	if rec := doGet(t, newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic)).Handler(),
		reportsPath+"/nope/events", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want pdbmux's own 404 when nobody holds the report", rec.Code)
	}

	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	rec := doGet(t, newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic)).Handler(),
		reportsPath+"/nope/events", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the upstream 400 replayed: %s", rec.Code, rec.Body.String())
	}
}

func TestHandler_RejectedQueryIsNotADegradedRound(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := doGet(t, srv.Handler(), nodesPath, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	hr := health(t, srv)
	if hr.Query.Partial || hr.Query.PartialRounds != 0 || hr.Query.LastPartial != "" {
		t.Fatalf("a refused query was counted as degraded service: %+v", hr.Query)
	}
	// The report's own reachability probe is a separate query the backends also
	// refuse, so they read as unreachable while the estate itself is fine.
	if hr.Status != "down" {
		t.Errorf("status = %q, want the probe's own verdict", hr.Status)
	}
}

func TestHandler_RejectedQueryIsNotCached(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	rec := doGet(t, srv.Handler(), factsPath, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the upstream 400 replayed: %s", rec.Code, rec.Body.String())
	}
	if n := srv.factsCache.Stats().Entries; n != 0 {
		t.Fatalf("cache holds %d entries after a rejected query", n)
	}

	// The rejection left nothing behind, so the next good query is served fresh.
	a.reject, b.reject = 0, 0
	a.factsBody = `[` + fact("h1", "role", "web", "") + `]`
	ok := doGet(t, srv.Handler(), factsPath, "")
	if ok.Code != http.StatusOK || !strings.Contains(ok.Body.String(), "web") {
		t.Fatalf("follow-up = %d %s", ok.Code, ok.Body.String())
	}
}

func TestHandler_AllBackendsFailedIsNotCached(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.fail, b.fail = true, true
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	if rec := doGet(t, srv.Handler(), factsPath, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if n := srv.factsCache.Stats().Entries; n != 0 {
		t.Fatalf("cache holds %d entries after a failed fan-out", n)
	}
}

// Stale records answer an outage. They do not answer a query the estate refused:
// the client has to be told why, not handed data for a question it did not ask.
func TestHandler_RejectedQueryIsNotAnsweredFromStale(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[`+fact("h1", "role", "old", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	if warm := doGet(t, srv.Handler(), factsPath, ""); warm.Code != http.StatusOK {
		t.Fatalf("warm-up status %d", warm.Code)
	}

	clk.advance(31 * time.Second)
	a.reject, a.rejectBody = http.StatusBadRequest, badOrderBy
	b.reject, b.rejectBody = http.StatusBadRequest, badOrderBy
	rec := doGet(t, srv.Handler(), factsPath, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the rejection rather than the stale entry: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "old") {
		t.Errorf("body = %s, want the rejection, not cached records", rec.Body.String())
	}
	if serving, _, _ := srv.stale.snapshot(); serving {
		t.Error("a refused query must not mark the cache as serving stale")
	}
}

// An outage still falls back to the stale copy, unchanged by the replay path.
func TestHandler_OutageStillFallsBackToStale(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[`+fact("h1", "role", "old", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	if warm := doGet(t, srv.Handler(), factsPath, ""); warm.Code != http.StatusOK {
		t.Fatalf("warm-up status %d", warm.Code)
	}
	clk.advance(31 * time.Second)
	a.fail, b.fail = true, true
	rec := doGet(t, srv.Handler(), factsPath, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "old") {
		t.Fatalf("stale fallback = %d %s", rec.Code, rec.Body.String())
	}
}

func TestWriteUpstreamError_RedactsBackendAddresses(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	leak := "connection to " + a.srv.URL + "/pdb/query/v4/nodes refused"
	a.reject, a.rejectBody = http.StatusBadRequest, leak
	b.reject, b.rejectBody = http.StatusBadRequest, leak
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	body := rec.Body.String()
	if strings.Contains(body, a.srv.URL) || strings.Contains(body, hostOf(t, a.srv.URL)) {
		t.Fatalf("replayed body names a backend: %q", body)
	}
	if !strings.Contains(body, redactedBackend) {
		t.Errorf("body = %q, want the address redacted", body)
	}
}

// The pass-through path replays upstream errors too, so it redacts the same way.
func TestProxyUnmerged_RedactsBackendAddresses(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, "upstream "+a.srv.URL+" said no"
	b.reject, b.rejectBody = http.StatusBadRequest, "upstream "+b.srv.URL+" said no"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), resourcesPath, `["=","type","File"]`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the upstream 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), hostOf(t, a.srv.URL)) {
		t.Errorf("replayed body names a backend: %q", rec.Body.String())
	}
}

func TestWriteUpstreamError_EmptyBodyGetsAMessage(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusBadRequest, ""
	b.reject, b.rejectBody = http.StatusBadRequest, ""
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) == "" {
		t.Error("a replayed rejection must carry some explanation")
	}
}

// clientShaped calls 406 and 415 replayable on the grounds that neither is
// reachable while the fan-out sends a bare GET. Forwarding a client's
// negotiation headers would make them reachable, so pin the premise.
func TestFanOut_ForwardsNoNegotiationHeaders(t *testing.T) {
	var mu sync.Mutex
	var seen []http.Header
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Clone())
		mu.Unlock()
		setContentType(w, "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer up.Close()

	h := newTestServer(testConfig(up.URL, up.URL, mergeStatic)).Handler()
	for _, path := range []string{nodesPath, resourcesPath} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", "application/xml")
		req.Header.Set("Content-Encoding", "br")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("no backend request observed")
	}
	for _, hdr := range seen {
		for _, name := range []string{"Accept", "Content-Encoding", "Content-Type"} {
			if v := hdr.Get(name); v != "" {
				t.Errorf("fan-out forwarded %s: %q", name, v)
			}
		}
	}
}

// Redaction is fail-safe: it over-matches rather than let an address through,
// and DNS case must not be a way past it.
func TestRedactBackends_Vectors(t *testing.T) {
	srv := newTestServer(testConfig("http://pdb1.example.net:8080", "https://198.51.100.40:8081", mergeStatic))
	for _, tc := range []struct {
		name, in, want string
	}{
		{"url", "GET http://pdb1.example.net:8080/pdb failed", "GET <backend>/pdb failed"},
		{"uppercase hostname", "PDB1.EXAMPLE.NET refused", "<backend> refused"},
		{"mixed-case url", "HTTP://PDB1.Example.Net:8080/pdb", "<backend>/pdb"},
		{"ip backend", "dial https://198.51.100.40:8081/pdb", "dial <backend>/pdb"},
		{"json-escaped slashes", `{"at":"http:\/\/pdb1.example.net:8080\/pdb"}`, `{"at":"http:\/\/<backend>\/pdb"}`},
		{"percent-encoded colon", "pdb1.example.net%3A8080", "<backend>%3A8080"},
		{"trailing-dot fqdn", "pdb1.example.net. timed out", "<backend>. timed out"},
		{"hostname as substring", "peer-pdb1.example.net-alt", "peer-<backend>-alt"},
		{"repeated", "pdb1.example.net and PDB1.example.net", "<backend> and <backend>"},
		{"non-ascii body survives folding", "Ünïcode PDB1.EXAMPLE.NET ✓", "Ünïcode <backend> ✓"},
		{"names nothing", "Unrecognized column 'bogus'", "Unrecognized column 'bogus'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(srv.redactBackends([]byte(tc.in))); got != tc.want {
				t.Errorf("redactBackends(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func hostOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parsing %q: %v", raw, err)
	}
	return u.Host
}
