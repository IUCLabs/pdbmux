package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// sourceValues returns certname -> value of the synthetic fact record, and the
// number of records carrying that fact name.
func sourceValues(t *testing.T, body []byte, factName string) (map[string]string, int) {
	t.Helper()
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	out := map[string]string{}
	n := 0
	for _, raw := range raws {
		var m struct {
			Certname string `json:"certname"`
			Name     string `json:"name"`
			Value    string `json:"value"`
		}
		if json.Unmarshal(raw, &m) != nil || m.Name != factName {
			continue
		}
		out[m.Certname] = m.Value
		n++
	}
	return out, n
}

// nodeSources returns certname -> the stamped provenance field on /nodes records.
func nodeSources(t *testing.T, body []byte, field string) map[string]string {
	t.Helper()
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	out := map[string]string{}
	for _, raw := range raws {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("unmarshal record %s: %v", raw, err)
		}
		var m recordMeta
		_ = json.Unmarshal(raw, &m)
		v, ok := obj[field]
		if !ok {
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			t.Fatalf("provenance field of %s is not a string: %v", raw, err)
		}
		out[m.Certname] = s
	}
	return out
}

func factEnv(cn, name, val, env string) string {
	return `{"certname":"` + cn + `","name":"` + name + `","value":"` + val + `","environment":"` + env + `"}`
}

// Both backends hold h1; a holds its newer report, so h1's facts and its
// provenance fact must both come from a.
func TestHandler_FactsSourceFollowsMergeOwner(t *testing.T) {
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`,`+node("h2", "2026-07-01T00:00:00Z")+`]`,
		`[`+factEnv("h1", "role", "web-a", "production")+`,`+factEnv("h2", "role", "db-a", "production")+`]`)
	b := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-20T00:00:00Z")+`]`,
		`[`+factEnv("h1", "role", "web-b", "staging")+`,`+factEnv("h2", "role", "db-b", "staging")+`]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	rec := doGet(t, srv.Handler(), factsPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 2 {
		t.Fatalf("expected one %s record per certname, got %d: %s", defaultSourceFact, n, rec.Body.String())
	}
	if got["h1"] != "a" || got["h2"] != "b" {
		t.Errorf("provenance must name the backend that won the merge, got %v", got)
	}
}

// The synthetic record carries the node's own environment so it groups with the
// real facts rather than landing in an unrelated environment.
func TestHandler_FactsSourceCopiesEnvironment(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+factEnv("h1", "role", "web", "staging")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, "")
	var raws []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raws); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, raw := range raws {
		var m recordMeta
		if json.Unmarshal(raw, &m) != nil || m.Name != defaultSourceFact {
			continue
		}
		found = true
		if m.Environment != "staging" {
			t.Errorf("environment = %q, want staging: %s", m.Environment, raw)
		}
	}
	if !found {
		t.Fatalf("no %s record: %s", defaultSourceFact, rec.Body.String())
	}
}

// A node record's provenance names the backend whose node record won /nodes'
// own report_timestamp merge.
func TestHandler_NodesSourceStamped(t *testing.T) {
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-01T00:00:00Z")+`,`+node("h2", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := nodeSources(t, rec.Body.Bytes(), defaultSourceFact)
	if got["h1"] != "b" || got["h2"] != "a" {
		t.Errorf("node provenance = %v, want h1=b h2=a", got)
	}
}

// Stamping must not drop unknown upstream fields.
func TestHandler_NodesSourceKeepsUpstreamFields(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	var raws []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raws); err != nil {
		t.Fatal(err)
	}
	if len(raws) != 1 {
		t.Fatalf("expected 1 node, got %d", len(raws))
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raws[0], &obj); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"certname", "report_timestamp", "latest_report_status", defaultSourceFact} {
		if _, ok := obj[k]; !ok {
			t.Errorf("field %q missing from stamped record: %s", k, raws[0])
		}
	}
}

func TestHandler_SourceDisabled(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	cfg := testConfig(a.srv.URL, b.srv.URL, mergeStatic)
	cfg.SourceFactEnabled = false
	srv := newTestServer(cfg)

	facts := doGet(t, srv.Handler(), factsPath, "")
	if _, n := sourceValues(t, facts.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("disabled injection still produced %d records: %s", n, facts.Body.String())
	}
	nodes := doGet(t, srv.Handler(), nodesPath, "")
	if got := nodeSources(t, nodes.Body.Bytes(), defaultSourceFact); len(got) != 0 {
		t.Errorf("disabled injection still stamped nodes: %v", got)
	}
}

func TestHandler_SourceFactNameOverride(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	cfg := testConfig(a.srv.URL, b.srv.URL, mergeStatic)
	cfg.SourceFact = "origin_pdb"
	srv := newTestServer(cfg)

	facts := doGet(t, srv.Handler(), factsPath, "")
	got, n := sourceValues(t, facts.Body.Bytes(), "origin_pdb")
	if n != 1 || got["h1"] != "a" {
		t.Errorf("override name not honoured: %s", facts.Body.String())
	}
	if _, n := sourceValues(t, facts.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("default name still emitted alongside the override: %s", facts.Body.String())
	}

	nodes := doGet(t, srv.Handler(), nodesPath, "")
	if got := nodeSources(t, nodes.Body.Bytes(), "origin_pdb"); got["h1"] != "a" {
		t.Errorf("override name not honoured on /nodes: %v", got)
	}
}

// An upstream fact of the configured name is replaced, not duplicated: pdbmux's
// own attribution is authoritative.
func TestHandler_UpstreamSourceFactOverridden(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web", "")+`,`+fact("h1", defaultSourceFact, "stale-value", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, "")
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 1 {
		t.Fatalf("expected exactly 1 %s record, got %d: %s", defaultSourceFact, n, rec.Body.String())
	}
	if got["h1"] != "a" {
		t.Errorf("upstream value survived: %v", got)
	}
}

// A count() must report the backends' real fact count, not one inflated by a
// record pdbmux invented.
func TestHandler_SourceNotInjectedOnAggregate(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factsPath] = `[{"count":3}]`
	b.bodies[factsPath] = `[{"count":2}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, `["extract",[["function","count"]]]`)
	if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("aggregate response gained %d synthetic records: %s", n, rec.Body.String())
	}
	// Every row must be one an upstream actually returned: an aggregate row pdbmux
	// invented or rewrote would change the count the client sees.
	var raws []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raws); err != nil {
		t.Fatal(err)
	}
	upstream := []string{`{"count":3}`, `{"count":2}`}
	for _, raw := range raws {
		if !slices.Contains(upstream, string(raw)) {
			t.Errorf("aggregate row %s is not an upstream row", raw)
		}
	}
}

// /nodes aggregates are summed rather than merged, so nothing may stamp them.
func TestHandler_NodesAggregateNotStamped(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.bodies[nodesPath] = `[{"count":3}]`
	b.bodies[nodesPath] = `[{"count":2}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, `["extract",[["function","count"]]]`)
	if strings.Contains(rec.Body.String(), defaultSourceFact) {
		t.Errorf("aggregate rows were stamped: %s", rec.Body.String())
	}
	if got := rec.Body.String(); !strings.Contains(got, `"count":5`) {
		t.Errorf("count = %s, want the summed 5", got)
	}
}

// A plain extract projects columns and skips the aggregate path, so the stamp
// must not add a key the client did not ask for.
func TestHandler_NodesProjectionNotStamped(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.bodies[nodesPath] = `[{"certname":"h1"}]`
	b.bodies[nodesPath] = `[]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, `["extract",["certname"]]`)
	if strings.Contains(rec.Body.String(), defaultSourceFact) {
		t.Errorf("projection gained a stamp: %s", rec.Body.String())
	}
}

// A query naming a specific fact asked for that fact only.
func TestHandler_SourceNotInjectedWhenNameFiltered(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, q := range []string{
		`["=","name","role"]`,
		`["and",["=","certname","h1"],["=","name","role"]]`,
		`["=","name","` + defaultSourceFact + `"]`,
	} {
		rec := doGet(t, srv.Handler(), factsPath, q)
		if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 0 {
			t.Errorf("query %s gained %d synthetic records: %s", q, n, rec.Body.String())
		}
	}
}

// A certname filter selects nodes, not facts, so the full fact set — synthetic
// record included — is still the right answer.
func TestHandler_SourceInjectedWhenOnlyCertnameFiltered(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, `["=","certname","h1"]`)
	if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 1 {
		t.Errorf("expected 1 synthetic record, got %d: %s", n, rec.Body.String())
	}
}

func TestInjectable(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		factEntity bool
		want       bool
	}{
		{"empty query", "", true, true},
		{"certname filter", `["=","certname","h1"]`, true, true},
		{"regex certname filter", `["~","certname","^web"]`, true, true},
		{"name filter", `["=","name","os"]`, true, false},
		{"name regex filter", `["~","name","^net"]`, true, false},
		{"name under and", `["and",["=","certname","h1"],["=","name","os"]]`, true, false},
		{"name under or", `["or",["=","name","os"],["=","name","kernel"]]`, true, false},
		{"name under not", `["not",["=","name","os"]]`, true, false},
		{"name in list", `["in","name",["array",["os"]]]`, true, false},
		// A select_facts subquery narrows which nodes match; the outer response is
		// still whole fact sets, so the synthetic record belongs in it.
		{"name only inside subquery", `["in","certname",["extract",["certname"],["select_facts",["=","name","os"]]]]`, true, true},
		{"top-level extract", `["extract",["certname","value"],["=","certname","h1"]]`, true, false},
		{"aggregate extract", `["extract",[["function","count"]]]`, true, false},
		{"nodes name filter is not a fact filter", `["=","name","os"]`, false, true},
		{"nodes extract", `["extract",["certname"]]`, false, false},
		{"unparseable query", `not json`, true, false},
		{"non-array query", `{"a":1}`, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := injectable(tc.query, tc.factEntity); got != tc.want {
				t.Errorf("injectable(%s, %v) = %v, want %v", tc.query, tc.factEntity, got, tc.want)
			}
		})
	}
}

// pypuppetdb reads certname/name/value/environment by direct index, so a
// missing key is a KeyError there — all four are always present.
func TestSourceInjector_FactRecordHasEveryFactKey(t *testing.T) {
	si := &sourceInjector{name: defaultSourceFact}
	for _, env := range []string{"production", ""} {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(si.factRecord("h1", "a", env), &obj); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"certname", "name", "value", "environment"} {
			if _, ok := obj[k]; !ok {
				t.Errorf("environment=%q: key %q missing from synthetic fact", env, k)
			}
		}
		if len(obj) != 4 {
			t.Errorf("synthetic fact has %d keys, want the 4 of a real fact record: %v", len(obj), obj)
		}
	}
}

// A nil injector is the disabled path and must leave every input untouched.
func TestSourceInjector_NilIsInert(t *testing.T) {
	var si *sourceInjector
	if si.claims(defaultSourceFact) {
		t.Error("nil injector claims a fact name")
	}
	if si.factRecord("h1", "a", "production") != nil {
		t.Error("nil injector produced a record")
	}
	raw := json.RawMessage(`{"certname":"h1"}`)
	if got := si.stamp(raw, "a"); string(got) != string(raw) {
		t.Errorf("nil injector rewrote %s to %s", raw, got)
	}
}

// A response element that is not a JSON object cannot be stamped, and must be
// passed through rather than dropped or mangled.
func TestSourceInjector_StampNonObject(t *testing.T) {
	si := &sourceInjector{name: defaultSourceFact}
	for _, raw := range []string{`"scalar"`, `[1,2]`, `null`} {
		if got := si.stamp(json.RawMessage(raw), "a"); string(got) != raw {
			t.Errorf("stamp(%s) = %s, want unchanged", raw, got)
		}
	}
}

func TestMergeFacts_SourceOrderedAfterOwnersFacts(t *testing.T) {
	a := recs(t, "a", fact("h1", "role", "web-a", ""), fact("h1", "kernel", "Linux", ""))
	b := recs(t, "b", fact("h1", "role", "web-b", ""))
	merged := mergeFacts([]backendResult{a, b}, nil, &sourceInjector{name: defaultSourceFact})

	got := factValues(t, merged)
	want := []string{"h1:role=web-a", "h1:kernel=Linux", "h1:" + defaultSourceFact + "=a"}
	if !slices.Equal(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}
