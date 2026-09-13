package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// The bodies below are the shapes openvoxdb 8.9.0 actually serves, captured
// from a live backend: a bare object for /factsets/<certname>
// (src/puppetlabs/puppetdb/http/handlers.clj:98-108,342-343), a fact array for
// /nodes/<certname>/facts (handlers.clj:373-376) and a pretty-printed
// {"error": ...} body for a certname the backend has no row for
// (http.clj:238-242 for the singleton, middleware.clj:381-398 for the child
// path, which names the parent — "node" — rather than the child).

func factsetBody(certname, timestamp, role string) string {
	return fmt.Sprintf(`{
  "timestamp" : %q,
  "facts" : {
    "data" : [ {
      "name" : "role",
      "value" : %q
    } ]
  },
  "certname" : %q,
  "hash" : "d775f92fb438b96276e721bb09524e6361156b06",
  "producer_timestamp" : %q,
  "producer" : "compiler.example.net",
  "environment" : "develop"
}`, timestamp, role, certname, timestamp)
}

func nodeFactsBody(certname, role string) string {
	return fmt.Sprintf(`[{"certname":%q,"environment":"develop","name":"role","value":%q}]`, certname, role)
}

func notFoundBody(kind, id string) string {
	return fmt.Sprintf("{\n  \"error\" : \"No information is known about %s %s\"\n}", kind, id)
}

// voxBackend answers the per-certname routes as openvoxdb does: the certnames
// it holds get a bare object or a fact array, and every other certname gets the
// parent-check 404. Each backend labels its facts with its own name, so a
// response says which backend answered it.
type voxBackend struct {
	srv  *httptest.Server
	name string
	// nodes maps a certname it holds to that node's report_timestamp.
	nodes map[string]string
	fail  bool
	// perNodeStatus and perNodeBody answer the per-certname routes with a fixed
	// reply, leaving /nodes alone so the freshness map still resolves an owner.
	perNodeStatus int
	perNodeBody   string
	// perNodeDead aborts the per-certname routes without a status, standing in
	// for a backend that is unreachable only for that request.
	perNodeDead bool

	mu        sync.Mutex
	gotParams map[string]url.Values
}

func newVoxBackend(t *testing.T, name string, nodes map[string]string) *voxBackend {
	t.Helper()
	vb := &voxBackend{name: name, nodes: nodes, gotParams: map[string]url.Values{}}
	vb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vb.mu.Lock()
		vb.gotParams[r.URL.Path] = r.URL.Query()
		vb.mu.Unlock()
		if vb.fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json;charset=utf-8")

		if isCertnameRoute(r.URL.Path) {
			if vb.perNodeDead {
				panic(http.ErrAbortHandler)
			}
			if vb.perNodeStatus != 0 {
				w.WriteHeader(vb.perNodeStatus)
				_, _ = io.WriteString(w, vb.perNodeBody)
				return
			}
		}

		if certname, ok := strings.CutPrefix(r.URL.Path, factsetsPath+"/"); ok {
			ts, held := vb.nodes[certname]
			if !held {
				vb.notFound(w, "factset", certname)
				return
			}
			_, _ = io.WriteString(w, factsetBody(certname, ts, vb.name))
			return
		}
		if rest, ok := strings.CutPrefix(r.URL.Path, nodesPath+"/"); ok {
			certname, _, _ := strings.Cut(rest, "/")
			if _, held := vb.nodes[certname]; !held {
				vb.notFound(w, "node", certname)
				return
			}
			_, _ = io.WriteString(w, nodeFactsBody(certname, vb.name))
			return
		}

		switch r.URL.Path {
		case nodesPath:
			_, _ = io.WriteString(w, "["+strings.Join(vb.nodeRecords(), ",")+"]")
		case factsPath:
			_, _ = io.WriteString(w, "["+strings.Join(vb.factRecords(), ",")+"]")
		default:
			_, _ = io.WriteString(w, "[]")
		}
	}))
	t.Cleanup(vb.srv.Close)
	return vb
}

func (vb *voxBackend) notFound(w http.ResponseWriter, kind, id string) {
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, notFoundBody(kind, id))
}

func (vb *voxBackend) nodeRecords() []string {
	out := make([]string, 0, len(vb.nodes))
	for certname, ts := range vb.nodes {
		out = append(out, node(certname, ts))
	}
	return out
}

func (vb *voxBackend) factRecords() []string {
	out := make([]string, 0, len(vb.nodes))
	for certname := range vb.nodes {
		out = append(out, fact(certname, "role", vb.name, ""))
	}
	return out
}

func (vb *voxBackend) params(path string) url.Values {
	vb.mu.Lock()
	defer vb.mu.Unlock()
	return vb.gotParams[path]
}

// perNodePaths are the routes a certname resolves on, with the body field that
// names the backend that answered.
var perNodePaths = []struct {
	name string
	path func(certname string) string
}{
	{"factsets/<certname>", func(cn string) string { return factsetsPath + "/" + cn }},
	{"nodes/<certname>/facts", func(cn string) string { return nodesPath + "/" + cn + "/facts" }},
}

func TestCertnameRoutes_ServeTheBackendHoldingTheNode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		aNodes   map[string]string
		bNodes   map[string]string
		certname string
		want     string
	}{
		{
			name:     "held only by a",
			aNodes:   map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"},
			bNodes:   map[string]string{},
			certname: "h1.example.net",
			want:     "a",
		},
		{
			name:     "held only by b",
			aNodes:   map[string]string{},
			bNodes:   map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"},
			certname: "h1.example.net",
			want:     "b",
		},
		{
			// b reported later, so b owns the node even though a is configured
			// first and would answer a plain pass-through.
			name:     "held by both, newer report on b",
			aNodes:   map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"},
			bNodes:   map[string]string{"h1.example.net": "2026-07-20T00:00:00Z"},
			certname: "h1.example.net",
			want:     "b",
		},
		{
			name:     "held by both, newer report on a",
			aNodes:   map[string]string{"h1.example.net": "2026-07-20T00:00:00Z"},
			bNodes:   map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"},
			certname: "h1.example.net",
			want:     "a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newVoxBackend(t, "a", tc.aNodes)
			b := newVoxBackend(t, "b", tc.bNodes)
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

			for _, rt := range perNodePaths {
				rec := doGet(t, srv.Handler(), rt.path(tc.certname), "")
				if rec.Code != http.StatusOK {
					t.Fatalf("%s: status %d: %s", rt.name, rec.Code, rec.Body.String())
				}
				if got := rec.Body.String(); !strings.Contains(got, `"`+tc.want+`"`) {
					t.Errorf("%s answered by the wrong backend, want %q: %s", rt.name, tc.want, got)
				}
			}

			// The collection merge has to agree: one node cannot report one
			// backend's facts here and another's through /facts.
			rec := doGet(t, srv.Handler(), factsPath, `["=","certname","`+tc.certname+`"]`)
			if got := factRoleValue(t, rec.Body.Bytes(), tc.certname); got != tc.want {
				t.Errorf("/facts resolved %s to %q, want %q; the routes disagree", tc.certname, got, tc.want)
			}
		})
	}
}

// factRoleValue reads the certname's "role" fact out of a merged /facts body.
func factRoleValue(t *testing.T, body []byte, certname string) string {
	t.Helper()
	var recs []struct {
		Certname string `json:"certname"`
		Name     string `json:"name"`
		Value    string `json:"value"`
	}
	if err := json.Unmarshal(body, &recs); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	for _, rec := range recs {
		if rec.Certname == certname && rec.Name == "role" {
			return rec.Value
		}
	}
	t.Fatalf("no role fact for %s in %s", certname, body)
	return ""
}

// A certname neither backend holds must still look like openvoxdb: its own 404
// status and error body, not an empty success.
func TestCertnameRoutes_UnknownCertnameKeepsTheUpstream404(t *testing.T) {
	a := newVoxBackend(t, "a", map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"})
	b := newVoxBackend(t, "b", map[string]string{"h2.example.net": "2026-07-01T00:00:00Z"})
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	for _, tc := range []struct {
		path string
		want string
	}{
		{factsetsPath + "/gone.example.net", notFoundBody("factset", "gone.example.net")},
		{nodesPath + "/gone.example.net/facts", notFoundBody("node", "gone.example.net")},
	} {
		rec := doGet(t, srv.Handler(), tc.path, "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404: %s", tc.path, rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != tc.want {
			t.Errorf("%s: body %q, want %q", tc.path, got, tc.want)
		}
		if got := rec.Header().Get("Content-Type"); got != "application/json;charset=utf-8" {
			t.Errorf("%s: content type %q", tc.path, got)
		}
	}
}

// A backend that is down is indistinguishable from one that has never heard of
// the node, so the surviving backend still answers.
func TestCertnameRoutes_OneBackendDown(t *testing.T) {
	a := newVoxBackend(t, "a", map[string]string{"h1.example.net": "2026-07-20T00:00:00Z"})
	b := newVoxBackend(t, "b", map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"})
	// a owns h1 by freshness, so the fallback is exercised only if a is skipped.
	a.fail = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	for _, rt := range perNodePaths {
		rec := doGet(t, srv.Handler(), rt.path("h1.example.net"), "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d, want the survivor's 200: %s", rt.name, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"b"`) {
			t.Errorf("%s: want b's record, got %s", rt.name, rec.Body.String())
		}
	}
}

// Static merge attributes no certname to a backend, so these routes keep the
// configured order the /facts merge falls back to.
func TestCertnameRoutes_StaticMergeKeepsConfiguredOrder(t *testing.T) {
	a := newVoxBackend(t, "a", map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"})
	b := newVoxBackend(t, "b", map[string]string{"h1.example.net": "2026-07-20T00:00:00Z"})
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsetsPath+"/h1.example.net", "")
	if !strings.Contains(rec.Body.String(), `"a"`) {
		t.Errorf("static merge should keep the first configured backend, got %s", rec.Body.String())
	}
}

// openvoxdb ANDs the request's own query onto the certname restriction
// (src/puppetlabs/puppetdb/http/query.clj:136-143,158-164), so the owner has to
// receive it unchanged.
func TestCertnameRoutes_ForwardQueryToTheOwner(t *testing.T) {
	a := newVoxBackend(t, "a", map[string]string{"h1.example.net": "2026-07-20T00:00:00Z"})
	b := newVoxBackend(t, "b", map[string]string{"h1.example.net": "2026-07-01T00:00:00Z"})
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	const q = `["=","name","role"]`
	path := nodesPath + "/h1.example.net/facts"
	if rec := doGet(t, srv.Handler(), path, q); rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := a.params(path).Get("query"); got != q {
		t.Errorf("owner got query %q, want %q", got, q)
	}
	if got := b.params(path).Get("query"); got != "" {
		t.Errorf("non-owner was queried with %q; only the owner should be asked", got)
	}
}

func TestCertnameFor(t *testing.T) {
	for _, tc := range []struct{ path, want string }{
		{factsetsPath + "/h1.example.net", "h1.example.net"},
		{nodesPath + "/h1.example.net", "h1.example.net"},
		{nodesPath + "/h1.example.net/facts", "h1.example.net"},
		{nodesPath + "/h1.example.net/facts/role/web", "h1.example.net"},
		{nodesPath + "/h1.example.net/resources", "h1.example.net"},
		{catalogsPath + "/h1.example.net/edges", "h1.example.net"},
		// Collections and fact-name drilldowns are merged elsewhere and must
		// not be mistaken for a certname.
		{nodesPath, ""},
		{nodesPath + "/", ""},
		{factsPath, ""},
		{factsPath + "/role", ""},
		{factsetsPath, ""},
		{reportsPath + "/abc123/events", ""},
	} {
		if got := certnameFor(tc.path); got != tc.want {
			t.Errorf("certnameFor(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestCertnameRoutes_ClaimedByTheRouteTable(t *testing.T) {
	for _, path := range []string{
		factsetsPath + "/h1.example.net",
		nodesPath + "/h1.example.net",
		nodesPath + "/h1.example.net/facts",
		catalogsPath + "/h1.example.net",
	} {
		if got := routeFor(path).name; got != certnameRouteName {
			t.Errorf("routeFor(%q) = %q, want %q", path, got, certnameRouteName)
		}
	}
}

// The owner holds the node, so its answer is the answer: a non-owner's 404 only
// says the node is not its own, and demanding it agree turns the owner's real
// error into a 502 that describes neither backend.
func TestCertnameRoutes_OwnerAnswerWins(t *testing.T) {
	const certname = "h1.example.net"
	for _, tc := range []struct {
		name string
		// setUp configures the owner (a, which reported later) and the backend
		// that does not hold the node (b).
		setUp    func(a, b *voxBackend)
		wantCode int
		wantBody string
	}{
		{
			name:     "owner errors while the non-owner 404s",
			setUp:    func(a, b *voxBackend) { a.perNodeStatus, a.perNodeBody = http.StatusInternalServerError, "boom" },
			wantCode: http.StatusInternalServerError,
			wantBody: "boom",
		},
		{
			name: "owner and non-owner error differently",
			setUp: func(a, b *voxBackend) {
				a.perNodeStatus, a.perNodeBody = http.StatusInternalServerError, "boom"
				b.perNodeStatus, b.perNodeBody = http.StatusServiceUnavailable, "busy"
			},
			wantCode: http.StatusInternalServerError,
			wantBody: "boom",
		},
		{
			// The fallback chain is there so an unreachable owner does not take the
			// node with it; a backend that did answer explains more than a 502.
			name: "owner unreachable, fallback answers",
			setUp: func(a, b *voxBackend) {
				a.perNodeDead = true
				b.perNodeStatus, b.perNodeBody = http.StatusServiceUnavailable, "busy"
			},
			wantCode: http.StatusServiceUnavailable,
			wantBody: "busy",
		},
		{
			name: "owner succeeds while the non-owner errors",
			setUp: func(a, b *voxBackend) {
				b.perNodeStatus, b.perNodeBody = http.StatusInternalServerError, "boom"
			},
			wantCode: http.StatusOK,
			wantBody: `"a"`,
		},
		{
			// Nothing answered at all, so there is no upstream reply to replay.
			name:     "every backend unreachable",
			setUp:    func(a, b *voxBackend) { a.perNodeDead, b.perNodeDead = true, true },
			wantCode: http.StatusBadGateway,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newVoxBackend(t, "a", map[string]string{certname: "2026-07-20T00:00:00Z"})
			b := newVoxBackend(t, "b", map[string]string{})
			tc.setUp(a, b)
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

			for _, rt := range perNodePaths {
				rec := doGet(t, srv.Handler(), rt.path(certname), "")
				if rec.Code != tc.wantCode {
					t.Fatalf("%s: status = %d, want %d: %s", rt.name, rec.Code, tc.wantCode, rec.Body.String())
				}
				if tc.wantBody != "" && !strings.Contains(rec.Body.String(), tc.wantBody) {
					t.Errorf("%s: body = %q, want it to contain %q", rt.name, rec.Body.String(), tc.wantBody)
				}
			}
		})
	}
}
