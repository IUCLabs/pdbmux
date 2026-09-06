package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"testing"
)

const (
	roleFactPath  = factsPath + "/role"
	osFactPath    = factsPath + "/osfamily"
	sourceFactURL = factsPath + "/" + defaultSourceFact
)

func names(t *testing.T, body []byte) []string {
	t.Helper()
	var out []string
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	return out
}

func factNamesBody(names ...string) string {
	b, err := json.Marshal(names)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestFactsSubPath(t *testing.T) {
	for _, tc := range []struct {
		path       string
		wantName   string
		wantValue  string
		wantValued bool
	}{
		{factsPath, "", "", false},
		{factsPath + "/", "", "", false},
		{roleFactPath, "role", "", false},
		{roleFactPath + "/web", "role", "web", true},
		{roleFactPath + "/", "", "", false},
		{roleFactPath + "/web/extra", "", "", false},
		{nodesPath + "/h1/facts/role", "", "", false},
		{factNamesPath, "", "", false},
	} {
		name, value, valued := factsSubPath(tc.path)
		if name != tc.wantName || value != tc.wantValue || valued != tc.wantValued {
			t.Errorf("factsSubPath(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.path, name, value, valued, tc.wantName, tc.wantValue, tc.wantValued)
		}
	}
}

// The path route carries the same records /facts does, so it merges the same
// way: every backend's nodes survive and a shared node resolves to one owner.
func TestHandler_FactsByNameMerged(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[roleFactPath] = `[` + fact("h1", "role", "web-a", "") + `,` + fact("h2", "role", "db-a", "") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[roleFactPath] = `[` + fact("h1", "role", "web-b", "") + `,` + fact("h3", "role", "db-b", "") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), roleFactPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := map[string]string{}
	var rows []struct {
		Certname string `json:"certname"`
		Value    string `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", rec.Body.String(), err)
	}
	for _, row := range rows {
		got[row.Certname] = row.Value
	}
	want := map[string]string{"h1": "web-a", "h2": "db-a", "h3": "db-b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged %s = %v, want %v", roleFactPath, got, want)
	}
	if h := rec.Header().Get(backendsHeader); h != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, h)
	}
}

// The /<name>/<value> sub-route is the same entity with one more constraint, so
// it takes the same merge rather than falling through to the pass-through path.
func TestHandler_FactsByNameAndValueMerged(t *testing.T) {
	path := roleFactPath + "/web"
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = `[` + fact("h1", "role", "web", "") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[path] = `[` + fact("h3", "role", "web", "") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), path, "")
	var rows []recordMeta
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", rec.Body.String(), err)
	}
	if len(rows) != 2 {
		t.Fatalf("merged %s returned %d records, want both backends': %s", path, len(rows), rec.Body.String())
	}
	if h := rec.Header().Get(backendsHeader); h != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, h)
	}
}

// An aggregate row carries no certname, so the per-certname merge would keep one
// backend's row and drop the other's without any error or partial-backend signal.
func TestHandler_FactsByNameAggregateSummed(t *testing.T) {
	for _, path := range []string{roleFactPath, roleFactPath + "/web"} {
		t.Run(path, func(t *testing.T) {
			a := newFakeBackend(t, `[]`, `[]`)
			a.bodies[path] = `[{"count":7}]`
			b := newFakeBackend(t, `[]`, `[]`)
			b.bodies[path] = `[{"count":3}]`
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

			rec := doGet(t, srv.Handler(), path, `["extract",[["function","count"]]]`)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{10}) {
				t.Errorf("count = %v, want [10]", got)
			}
			if h := rec.Header().Get(backendsHeader); h != "2/2" {
				t.Errorf("%s = %q, want 2/2", backendsHeader, h)
			}
		})
	}
}

func TestHandler_FactsByNameAggregateGroupedSummed(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[roleFactPath] = `[{"count":2,"value":"web"},{"count":1,"value":"db"}]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[roleFactPath] = `[{"count":5,"value":"web"}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), roleFactPath,
		`["extract",[["function","count"],"value"],["group_by","value"]]`)
	var rows []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", rec.Body.String(), err)
	}
	got := map[string]float64{}
	for _, row := range rows {
		v, _ := row["value"].(string)
		n, _ := row["count"].(float64)
		got[v] = n
	}
	want := map[string]float64{"web": 7, "db": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grouped counts = %v, want %v", got, want)
	}
}

// The summing branch must not divert a plain query, including an extract
// projection that carries no function column.
func TestHandler_FactsByNameNonAggregateStillMergedByCertname(t *testing.T) {
	for _, q := range []string{"", `["extract",["certname","value"],["~","certname",".*"]]`} {
		a := newFakeBackend(t, `[]`, `[]`)
		a.bodies[roleFactPath] = `[` + fact("h1", "role", "web-a", "") + `,` + fact("h2", "role", "db-a", "") + `]`
		b := newFakeBackend(t, `[]`, `[]`)
		b.bodies[roleFactPath] = `[` + fact("h1", "role", "web-b", "") + `,` + fact("h3", "role", "db-b", "") + `]`
		srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

		rec := doGet(t, srv.Handler(), roleFactPath, q)
		var rows []recordMeta
		if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
			t.Fatalf("query %q: unmarshal %s: %v", q, rec.Body.String(), err)
		}
		got := map[string]bool{}
		for _, row := range rows {
			got[row.Certname] = true
		}
		if len(rows) != 3 || !got["h1"] || !got["h2"] || !got["h3"] {
			t.Errorf("query %q: merged %s = %s, want one record per certname", q, roleFactPath, rec.Body.String())
		}
	}
}

// A summed row is not the merged record set the cache stores, so it stays live.
func TestHandler_FactsByNameAggregateNotCached(t *testing.T) {
	for _, path := range []string{roleFactPath, roleFactPath + "/web", sourceFactURL} {
		t.Run(path, func(t *testing.T) {
			a := newCountingBackend(t, map[string]string{path: `[{"count":7}]`})
			b := newCountingBackend(t, map[string]string{path: `[{"count":3}]`})
			srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

			const q = `["extract",[["function","count"]]]`
			doGet(t, srv.Handler(), path, q)
			second := doGet(t, srv.Handler(), path, q)
			if got := a.hitCount(path); got != 2 {
				t.Errorf("%s aggregates are uncached: %d requests, want 2", path, got)
			}
			if got := counts(t, second.Body.Bytes(), "count"); !slices.Equal(got, []float64{10}) {
				t.Errorf("count = %v, want [10]", got)
			}
		})
	}
}

func TestHandler_FactsByNameForwardsPathAndQuery(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[roleFactPath] = `[]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[roleFactPath] = `[]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	q := `["=","certname","h1"]`
	doGet(t, srv.Handler(), roleFactPath, q)
	for _, fb := range []*fakeBackend{a, b} {
		if got := fb.gotQuery(roleFactPath); got != q {
			t.Errorf("backend got query %q on %s, want %q", got, roleFactPath, q)
		}
	}
}

// The path is a name constraint, the shape the injection gate already excludes.
func TestHandler_FactsByNameNotInjected(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[osFactPath] = `[` + fact("h1", "osfamily", "RedHat", "") + `]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[osFactPath] = `[` + fact("h2", "osfamily", "Debian", "") + `]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), osFactPath, "")
	if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("%s was injected into %s: %s", defaultSourceFact, osFactPath, rec.Body.String())
	}
}

// sourceFactBackends is a pair of fake backends whose /facts bodies give h1 to a
// and h2/h3 to b, the shape every drilldown assertion below rests on.
func sourceFactBackends(t *testing.T) (*fakeBackend, *fakeBackend) {
	t.Helper()
	a := newFakeBackend(t,
		`[`+node("h1", "2026-01-01T00:00:00Z")+`,`+node("h3", "2026-01-01T00:00:00Z")+`]`,
		`[`+factEnv("h1", "osfamily", "RedHat", "prod")+`,`+factEnv("h3", "osfamily", "RedHat", "prod")+`]`)
	b := newFakeBackend(t,
		`[`+node("h2", "2026-01-01T00:00:00Z")+`,`+node("h3", "2026-06-01T00:00:00Z")+`]`,
		`[`+factEnv("h2", "osfamily", "Debian", "dev")+`,`+factEnv("h3", "osfamily", "Debian", "dev")+`]`)
	return a, b
}

// The drilldown has to answer with the records /facts carries, which no backend
// holds: the certname set and the per-node owner come from the same merge.
func TestHandler_FactsBySourceNamePathSynthesisesOneRecordPerNode(t *testing.T) {
	for _, merge := range []string{mergeStatic, mergeFreshness} {
		t.Run(merge, func(t *testing.T) {
			a, b := sourceFactBackends(t)
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, merge))

			// h3 lives in both: static keeps the first configured backend, freshness
			// the one holding its newer report.
			wantH3 := "a"
			if merge == mergeFreshness {
				wantH3 = "b"
			}
			want := map[string]string{"h1": "a", "h2": "b", "h3": wantH3}

			rec := doGet(t, srv.Handler(), sourceFactURL, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
			if n != len(want) || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s = %v (%d records), want %v", sourceFactURL, got, n, want)
			}

			// The drilldown must agree with the records the unfiltered route reports.
			unfiltered, _ := sourceValues(t, doGet(t, srv.Handler(), factsPath, "").Body.Bytes(), defaultSourceFact)
			if !reflect.DeepEqual(got, unfiltered) {
				t.Errorf("%s = %v, want the same attribution %s reports: %v", sourceFactURL, got, factsPath, unfiltered)
			}
			if h := rec.Header().Get(backendsHeader); h != "2/2" {
				t.Errorf("%s = %q, want 2/2", backendsHeader, h)
			}
		})
	}
}

// A fact record's four keys are always present, and environment is the owning
// backend's, since that is the record the merge attributed the node to.
func TestHandler_FactsBySourceNamePathCarriesTheOwnersEnvironment(t *testing.T) {
	a, b := sourceFactBackends(t)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	rec := doGet(t, srv.Handler(), sourceFactURL, "")
	var rows []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", rec.Body.String(), err)
	}
	want := map[string]string{"h1": "prod", "h2": "dev", "h3": "dev"}
	for _, row := range rows {
		for _, key := range []string{"certname", "name", "value", "environment"} {
			if _, ok := row[key]; !ok {
				t.Fatalf("synthetic record %v is missing %s", row, key)
			}
		}
		cn, _ := row["certname"].(string)
		if got := row["environment"]; got != want[cn] {
			t.Errorf("environment of %s = %v, want the owner's %q", cn, got, want[cn])
		}
	}
}

// The pinned value names a backend, so the sub-route is the drilldown filtered
// by owner; a value naming no backend matches nothing.
func TestHandler_FactsBySourceNameAndValueFiltersByOwner(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  map[string]string
	}{
		{"a", map[string]string{"h1": "a"}},
		{"b", map[string]string{"h2": "b", "h3": "b"}},
		{"nosuchbackend", map[string]string{}},
	} {
		t.Run(tc.value, func(t *testing.T) {
			a, b := sourceFactBackends(t)
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

			rec := doGet(t, srv.Handler(), sourceFactURL+"/"+tc.value, "")
			got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
			if n != len(tc.want) || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s/%s = %v (%d records), want %v", sourceFactURL, tc.value, got, n, tc.want)
			}
		})
	}
}

// The pinned value is client-supplied and the drilldown's fetch is the widest
// query pdbmux makes, so a value naming no backend is answered from the
// configured names alone: no fan-out, and so no per-value cache key either.
func TestHandler_FactsBySourceNameUnknownValueSkipsFanOut(t *testing.T) {
	facts := map[string]string{factsPath: `[` + fact("h1", "osfamily", "RedHat", "") + `]`}
	a := newCountingBackend(t, facts)
	b := newCountingBackend(t, facts)
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	for _, value := range []string{"nosuchbackend", "nonce-1", "nonce-2"} {
		rec := doGet(t, srv.Handler(), sourceFactURL+"/"+value, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != "[]\n" {
			t.Errorf("%s/%s = %q, want %q", sourceFactURL, value, got, "[]\n")
		}
		// The answer is complete, not built from a subset of backends.
		if h := rec.Header().Get(backendsHeader); h != "2/2" {
			t.Errorf("%s/%s %s = %q, want 2/2", sourceFactURL, value, backendsHeader, h)
		}
	}
	for _, cb := range []*countingBackend{a, b} {
		if got := cb.totalHits(); got != 0 {
			t.Errorf("unknown value reached a backend %d times, want 0", got)
		}
	}
}

// The record set is the estate's, not the pinned value's, so every value shares
// one entry: two valid values must not cost two whole-estate fan-outs.
func TestHandler_FactsBySourceNameValuesShareOneFetch(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "osfamily", "RedHat", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h2", "osfamily", "Debian", "") + `]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	for _, tc := range []struct {
		path string
		want map[string]string
	}{
		{sourceFactURL + "/a", map[string]string{"h1": "a"}},
		{sourceFactURL + "/b", map[string]string{"h2": "b"}},
		{sourceFactURL, map[string]string{"h1": "a", "h2": "b"}},
	} {
		rec := doGet(t, srv.Handler(), tc.path, "")
		got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
		if n != len(tc.want) || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s = %v (%d records), want %v", tc.path, got, n, tc.want)
		}
	}
	for _, cb := range []*countingBackend{a, b} {
		if got := cb.hitCount(factsPath); got != 1 {
			t.Errorf("%s fetched %d times, want 1 shared fetch", factsPath, got)
		}
		if got := cb.hitCount(sourceFactURL); got != 0 {
			t.Errorf("%s was fanned out %d times, want 0", sourceFactURL, got)
		}
	}
}

// Nothing produces the fact while injection is off, so the drilldown is empty
// and the name is not advertised for a client to click through to.
func TestHandler_FactsBySourceNamePathEmptyWhenDisabled(t *testing.T) {
	a, b := sourceFactBackends(t)
	a.bodies[factNamesPath] = factNamesBody("osfamily")
	b.bodies[factNamesPath] = `[]`
	cfg := testConfig(a.srv.URL, b.srv.URL, mergeFreshness)
	cfg.SourceFactEnabled = false
	srv := newTestServer(cfg)

	// No backend holds the fact, so the route falls through to the plain merge.
	for _, fb := range []*fakeBackend{a, b} {
		fb.bodies[sourceFactURL] = `[]`
		fb.bodies[sourceFactURL+"/a"] = `[]`
	}
	for _, path := range []string{sourceFactURL, sourceFactURL + "/a"} {
		if got := doGet(t, srv.Handler(), path, "").Body.String(); got != "[]\n" {
			t.Errorf("disabled %s = %q, want %q", path, got, "[]\n")
		}
	}
	if got := names(t, doGet(t, srv.Handler(), factNamesPath, "").Body.Bytes()); !reflect.DeepEqual(got, []string{"osfamily"}) {
		t.Errorf("disabled %s = %v, want no %s entry", factNamesPath, got, defaultSourceFact)
	}
}

// The query narrows the certnames upstream, so the drilldown only covers what it
// selects.
func TestHandler_FactsBySourceNamePathRespectsQuery(t *testing.T) {
	a, b := sourceFactBackends(t)
	a.bodies[factsPath] = `[` + factEnv("h1", "osfamily", "RedHat", "prod") + `]`
	b.bodies[factsPath] = `[]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	const q = `["=","certname","h1"]`
	rec := doGet(t, srv.Handler(), sourceFactURL, q)
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 1 || got["h1"] != "a" {
		t.Fatalf("%s?query=%s = %v (%d records), want only h1: %s", sourceFactURL, q, got, n, rec.Body.String())
	}
	// The query has to reach the backends for them to narrow anything.
	for _, fb := range []*fakeBackend{a, b} {
		if seen := fb.gotQuery(factsPath); seen != q {
			t.Errorf("backend got query %q on %s, want %q", seen, factsPath, q)
		}
	}
}

// pdbmux owns the name, so an upstream record of it is dropped rather than
// listed beside the synthetic one.
func TestHandler_FactsBySourceNamePathSuppressesUpstream(t *testing.T) {
	a, b := sourceFactBackends(t)
	a.bodies[factsPath] = `[` + factEnv("h1", "osfamily", "RedHat", "prod") + `,` +
		fact("h1", defaultSourceFact, "stale", "") + `]`
	b.bodies[factsPath] = `[]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	rec := doGet(t, srv.Handler(), sourceFactURL, "")
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 1 || got["h1"] != "a" {
		t.Fatalf("%s = %v (%d records), want exactly the synthetic one: %s", sourceFactURL, got, n, rec.Body.String())
	}
}

// A name-constrained query on the source fact's own path is still gated by the
// query, not just the path.
func TestHandler_FactsBySourceNamePathRespectsQueryGate(t *testing.T) {
	a, b := sourceFactBackends(t)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), sourceFactURL, `["extract",["certname","value"]]`)
	if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("%s survived an extract projection: %s", defaultSourceFact, rec.Body.String())
	}
}

// An aggregate carries no certname to attribute, so the route stays on the
// summing path rather than synthesising records.
func TestHandler_FactsBySourceNameAggregateStillSummed(t *testing.T) {
	for _, path := range []string{sourceFactURL, sourceFactURL + "/a"} {
		t.Run(path, func(t *testing.T) {
			a, b := sourceFactBackends(t)
			a.bodies[path] = `[{"count":7}]`
			b.bodies[path] = `[{"count":3}]`
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

			rec := doGet(t, srv.Handler(), path, `["extract",[["function","count"]]]`)
			if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{10}) {
				t.Errorf("count = %v, want [10]", got)
			}
		})
	}
}

func TestHandler_FactNamesUnionDedupedAndSorted(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factNamesPath] = factNamesBody("kernel", "only_a", "osfamily")
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = factNamesBody("extra_b", "kernel", "only_b", "osfamily")
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factNamesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	want := []string{"extra_b", "kernel", "only_a", "only_b", "osfamily", defaultSourceFact}
	if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
		t.Errorf("merged %s = %v, want %v", factNamesPath, got, want)
	}
	if h := rec.Header().Get(backendsHeader); h != "2/2" {
		t.Errorf("%s = %q, want 2/2", backendsHeader, h)
	}
}

func TestHandler_FactNamesOrderByDescending(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factNamesPath] = factNamesBody("kernel", "osfamily")
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = factNamesBody("zone")
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), factNamesPath, url.Values{
		"order_by": {`[{"field":"name","order":"desc"}]`},
	})
	want := []string{"zone", defaultSourceFact, "osfamily", "kernel"}
	if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
		t.Errorf("descending %s = %v, want %v", factNamesPath, got, want)
	}
}

// Paging is applied to the merged list: each backend can only page its own.
func TestHandler_FactNamesPagedAcrossBackends(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factNamesPath] = factNamesBody("a1", "a2", "a3")
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = factNamesBody("b1", "b2")
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), factNamesPath, url.Values{
		"limit":  {"2"},
		"offset": {"1"},
	})
	want := []string{"a2", "a3"}
	if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
		t.Errorf("paged %s = %v, want %v", factNamesPath, got, want)
	}
	// Backends are asked for limit+offset with no offset, so the union has enough
	// records to page locally.
	if got, _ := a.params(factNamesPath); got.Get("limit") != "3" || got.Get("offset") != "" {
		t.Errorf("backend a got limit=%q offset=%q, want 3 and none", got.Get("limit"), got.Get("offset"))
	}
}

// A merged total counts the union, so the limit cannot be pushed upstream.
func TestHandler_FactNamesTotalCountsTheWholeUnion(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factNamesPath] = factNamesBody("a1", "a2", "a3")
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = factNamesBody("a3", "b1")
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), factNamesPath, url.Values{
		"limit":         {"2"},
		"include_total": {"true"},
	})
	if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, []string{"a1", "a2"}) {
		t.Errorf("paged %s = %v, want [a1 a2]", factNamesPath, got)
	}
	if got := rec.Header().Get(recordsHeader); got != "5" {
		t.Errorf("%s = %q, want the deduped union size 5", recordsHeader, got)
	}
	if got, _ := a.params(factNamesPath); got.Get("limit") != "" {
		t.Errorf("backend a got limit=%q, want none: a total cannot be counted from a window", got.Get("limit"))
	}
}

func TestHandler_FactNamesRejectsBadPaging(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, params := range []url.Values{
		{"limit": {"-1"}},
		// name is the only column the entity projects, so anything else is a 400
		// upstream and must be one here.
		{"order_by": {`[{"field":"bogus","order":"desc"}]`}},
	} {
		rec := doGetParams(t, srv.Handler(), factNamesPath, params)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%v: status %d, want 400: %s", params, rec.Code, rec.Body.String())
		}
	}
}

// The merged /facts response carries records of the name pdbmux owns, so the
// overview has to list it — and must not once nothing produces it.
func TestHandler_FactNamesListsTheOwnedName(t *testing.T) {
	for _, tc := range []struct {
		name     string
		enabled  bool
		upstream string
		want     []string
	}{
		{"enabled", true, factNamesBody("osfamily"), []string{"osfamily", defaultSourceFact}},
		{"disabled", false, factNamesBody("osfamily"), []string{"osfamily"}},
		{"listed once when a backend reports it too", true, factNamesBody(defaultSourceFact, "osfamily"), []string{"osfamily", defaultSourceFact}},
		{"kept when a backend reports it and the feature is off", false, factNamesBody(defaultSourceFact, "osfamily"), []string{"osfamily", defaultSourceFact}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newFakeBackend(t, `[]`, `[]`)
			a.bodies[factNamesPath] = tc.upstream
			b := newFakeBackend(t, `[]`, `[]`)
			b.bodies[factNamesPath] = `[]`
			cfg := testConfig(a.srv.URL, b.srv.URL, mergeStatic)
			cfg.SourceFactEnabled = tc.enabled
			srv := newTestServer(cfg)

			rec := doGet(t, srv.Handler(), factNamesPath, "")
			if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s = %v, want %v", factNamesPath, got, tc.want)
			}
		})
	}
}

// The owned name is part of the union, so it is counted and ordered with it.
func TestHandler_FactNamesOwnedNameIsCountedAndOrdered(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factNamesPath] = factNamesBody("osfamily", "zone")
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = `[]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGetParams(t, srv.Handler(), factNamesPath, url.Values{
		"order_by":      {`[{"field":"name","order":"desc"}]`},
		"include_total": {"true"},
	})
	want := []string{"zone", defaultSourceFact, "osfamily"}
	if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, want) {
		t.Errorf("descending %s = %v, want %v", factNamesPath, got, want)
	}
	if got := rec.Header().Get(recordsHeader); got != "3" {
		t.Errorf("%s = %q, want 3", recordsHeader, got)
	}
}

func TestHandler_FactNamesServesEmptyArray(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factNamesPath] = `[]`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = `[]`
	cfg := testConfig(a.srv.URL, b.srv.URL, mergeStatic)
	cfg.SourceFactEnabled = false
	srv := newTestServer(cfg)

	rec := doGet(t, srv.Handler(), factNamesPath, "")
	if got := rec.Body.String(); got != "[]\n" {
		t.Errorf("empty %s body = %q, want %q", factNamesPath, got, "[]\n")
	}
}

// A backend that is down must not black-hole the other's names.
func TestHandler_FactNamesSurvivesOneBackend(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[factNamesPath] = factNamesBody("kernel")
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factNamesPath, "")
	if got := names(t, rec.Body.Bytes()); !reflect.DeepEqual(got, []string{"kernel", defaultSourceFact}) {
		t.Errorf("%s = %v, want the surviving backend's names", factNamesPath, got)
	}
	if h := rec.Header().Get(backendsHeader); h != "1/2" {
		t.Errorf("%s = %q, want 1/2", backendsHeader, h)
	}
}

// Both routes are merged record sets, so they use the same cache /facts does.
func TestHandler_FactRoutesAreCached(t *testing.T) {
	for _, path := range []string{roleFactPath, roleFactPath + "/web", sourceFactURL, factNamesPath} {
		t.Run(path, func(t *testing.T) {
			a := newFakeBackend(t, `[]`, `[]`)
			a.bodies[path] = `[]`
			b := newFakeBackend(t, `[]`, `[]`)
			b.bodies[path] = `[]`
			srv := newTestServer(cacheTestConfig(a.srv.URL, b.srv.URL))

			if got := doGet(t, srv.Handler(), path, "").Header().Get(cacheStatusHeader); got != "miss" {
				t.Errorf("first %s = %q, want miss", cacheStatusHeader, got)
			}
			if got := doGet(t, srv.Handler(), path, "").Header().Get(cacheStatusHeader); got != "hit" {
				t.Errorf("second %s = %q, want hit", cacheStatusHeader, got)
			}
		})
	}
}
