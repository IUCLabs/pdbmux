package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func metaGet(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func metaString(t *testing.T, body []byte, field string) string {
	t.Helper()
	var obj map[string]string
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	return obj[field]
}

func TestMetaVersion_BackendsAgree(t *testing.T) {
	// Puppetboard's check_db_version() calls this at import and exits 2 on any
	// non-200, so a 404 here is the difference between a running dashboard and
	// CrashLoopBackOff.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[metaVersionPath] = `{"version":"7.12.1"}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaVersionPath] = `{"version":"7.12.1"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), metaVersionPath)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := metaString(t, rec.Body.Bytes(), "version"); got != "7.12.1" {
		t.Errorf("version = %q, want 7.12.1", got)
	}
}

func TestMetaVersion_DisagreementReportsLowest(t *testing.T) {
	// The estate can only be relied on for what its oldest PuppetDB implements.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[metaVersionPath] = `{"version":"8.4.0"}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaVersionPath] = `{"version":"7.12.1"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if got := metaString(t, metaGet(t, srv.Handler(), metaVersionPath).Body.Bytes(), "version"); got != "7.12.1" {
		t.Errorf("version = %q, want the lower 7.12.1", got)
	}
}

func TestMetaVersion_LowestIsIndependentOfBackendOrder(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[metaVersionPath] = `{"version":"7.12.1"}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaVersionPath] = `{"version":"8.4.0"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if got := metaString(t, metaGet(t, srv.Handler(), metaVersionPath).Body.Bytes(), "version"); got != "7.12.1" {
		t.Errorf("version = %q, want the lower 7.12.1", got)
	}
}

func TestMetaVersion_OneBackendDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaVersionPath] = `{"version":"8.4.0"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), metaVersionPath)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving the survivor, got %d", rec.Code)
	}
	if got := metaString(t, rec.Body.Bytes(), "version"); got != "8.4.0" {
		t.Errorf("version = %q, want 8.4.0", got)
	}
}

func TestMetaVersion_AllBackendsDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.dead = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.dead = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := metaGet(t, srv.Handler(), metaVersionPath); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502", rec.Code)
	}
}

// /pdb/meta had its own copy of the gateway error, so a status both backends
// agreed on never reached the client. It answers failures the way the query
// routes do now.
func TestMeta_ReplaysUnanimousUpstreamStatus(t *testing.T) {
	for _, path := range []string{metaVersionPath, metaServerTimePath} {
		for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusInternalServerError} {
			t.Run(path+"/"+http.StatusText(status), func(t *testing.T) {
				a := newFakeBackend(t, `[]`, `[]`)
				b := newFakeBackend(t, `[]`, `[]`)
				a.reject, a.rejectBody = status, "upstream said no"
				b.reject, b.rejectBody = status, "upstream said no"
				srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

				rec := metaGet(t, srv.Handler(), path)
				if rec.Code != status {
					t.Fatalf("status %d, want the upstream %d: %s", rec.Code, status, rec.Body.String())
				}
				if !strings.Contains(rec.Body.String(), "upstream said no") {
					t.Errorf("body = %q, want the upstream explanation", rec.Body.String())
				}
			})
		}
	}
}

func TestMeta_DisagreeingStatusesStay502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusNotFound, "gone"
	b.reject, b.rejectBody = http.StatusInternalServerError, "boom"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := metaGet(t, srv.Handler(), metaVersionPath); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502 when backends disagree", rec.Code)
	}
}

// A replayed body must not name a backend, on /pdb/meta as anywhere else.
func TestMeta_ReplayedBodyIsRedacted(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusInternalServerError, "upstream "+a.srv.URL+" blew up"
	b.reject, b.rejectBody = http.StatusInternalServerError, "upstream "+b.srv.URL+" blew up"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), metaVersionPath)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want the upstream 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), hostOf(t, a.srv.URL)) {
		t.Errorf("replayed body names a backend: %q", rec.Body.String())
	}
}

func TestMetaServerTime_FirstReachableBackend(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[metaServerTimePath] = `{"server_time":"2026-08-29T01:00:00.000Z"}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaServerTimePath] = `{"server_time":"2026-08-29T02:00:00.000Z"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	got := metaString(t, metaGet(t, srv.Handler(), metaServerTimePath).Body.Bytes(), "server_time")
	if got != "2026-08-29T01:00:00.000Z" {
		t.Errorf("server_time = %q, want the first backend's", got)
	}
}

func TestMetaServerTime_SkipsDeadBackend(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaServerTimePath] = `{"server_time":"2026-08-29T02:00:00.000Z"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), metaServerTimePath)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving the survivor, got %d", rec.Code)
	}
	if got := metaString(t, rec.Body.Bytes(), "server_time"); got != "2026-08-29T02:00:00.000Z" {
		t.Errorf("server_time = %q, want the survivor's", got)
	}
}

func TestMetaVersion_RejectsNonGET(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, metaVersionPath, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status %d, want 405", rec.Code)
	}
}

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"7.12.1", "7.12.1", 0},
		{"7.12.1", "8.4.0", -1},
		{"8.4.0", "7.12.1", 1},
		{"7.9.0", "7.12.0", -1}, // numeric, not lexical: 9 < 12
		{"7.12", "7.12.1", -1},
		{"8.0.0", "8.0.0-SNAPSHOT", -1},
		{"8.0.0-SNAPSHOT", "8.0.0", 1},
	}
	for _, c := range cases {
		if got := compareVersions(c.a, c.b); got != c.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestMetaField_MalformedBodyIgnored(t *testing.T) {
	// A backend serving junk must not become the "lowest" version.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[metaVersionPath] = `not json`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[metaVersionPath] = `{"version":"8.4.0"}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if got := metaString(t, metaGet(t, srv.Handler(), metaVersionPath).Body.Bytes(), "version"); got != "8.4.0" {
		t.Errorf("version = %q, want 8.4.0 from the only parseable backend", got)
	}
}
