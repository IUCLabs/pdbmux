package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.fail = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := metaGet(t, srv.Handler(), metaVersionPath); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502", rec.Code)
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
