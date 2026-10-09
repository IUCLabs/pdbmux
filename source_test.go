package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
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
	srv := newTestServer(hostedConfig(a.srv.URL, b.srv.URL, mergeFreshness))

	rec := doGet(t, srv.Handler(), factsPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 2 {
		t.Fatalf("expected one %s record per certname, got %d: %s", defaultSourceFact, n, rec.Body.String())
	}
	if got["h1"] != hostA || got["h2"] != hostB {
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
	srv := newTestServer(hostedConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), nodesPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := nodeSources(t, rec.Body.Bytes(), defaultSourceFact)
	if got["h1"] != hostB || got["h2"] != hostA {
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
	cfg := hostedConfig(a.srv.URL, b.srv.URL, mergeStatic)
	cfg.SourceFact = "origin_pdb"
	srv := newTestServer(cfg)

	facts := doGet(t, srv.Handler(), factsPath, "")
	got, n := sourceValues(t, facts.Body.Bytes(), "origin_pdb")
	if n != 1 || got["h1"] != hostA {
		t.Errorf("override name not honoured: %s", facts.Body.String())
	}
	if _, n := sourceValues(t, facts.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("default name still emitted alongside the override: %s", facts.Body.String())
	}

	nodes := doGet(t, srv.Handler(), nodesPath, "")
	if got := nodeSources(t, nodes.Body.Bytes(), "origin_pdb"); got["h1"] != hostA {
		t.Errorf("override name not honoured on /nodes: %v", got)
	}
}

// An upstream fact of the configured name is replaced, not duplicated: pdbmux's
// own attribution is authoritative.
func TestHandler_UpstreamSourceFactOverridden(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", "role", "web", "")+`,`+fact("h1", defaultSourceFact, "stale-value", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(hostedConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, "")
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 1 {
		t.Fatalf("expected exactly 1 %s record, got %d: %s", defaultSourceFact, n, rec.Body.String())
	}
	if got["h1"] != hostA {
		t.Errorf("upstream value survived: %v", got)
	}
}

// The configured name means one thing on every query shape: an upstream fact of
// that name is dropped whether or not the query gate allows synthesis.
func TestHandler_UpstreamSourceFactSuppressedOnEveryGateState(t *testing.T) {
	const upstream = "REAL-UPSTREAM-VALUE"
	tests := []struct {
		name  string
		query string
		want  string // synthetic value, or "" when the gate blocks injection
	}{
		{"injection on", "", hostA},
		{"gated by extract", `["extract",["certname","name","value"],["=","certname","h1"]]`, ""},
		{"gated by name filter", `["or",["=","name","role"],["=","name","os"]]`, ""},
		{"selected by name filter", `["or",["=","name","role"],["=","name","` + defaultSourceFact + `"]]`, hostA},
		{"gated by nested extract", `["and",["=","certname","h1"],["extract",["certname"]]]`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
				`[`+fact("h1", "role", "web", "")+`,`+fact("h1", defaultSourceFact, upstream, "")+`]`)
			b := newFakeBackend(t, `[]`, `[]`)
			srv := newTestServer(hostedConfig(a.srv.URL, b.srv.URL, mergeStatic))

			rec := doGet(t, srv.Handler(), factsPath, tc.query)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), upstream) {
				t.Fatalf("upstream value survived: %s", rec.Body.String())
			}
			got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
			if tc.want == "" {
				if n != 0 {
					t.Errorf("gated query produced %d %s records: %s", n, defaultSourceFact, rec.Body.String())
				}
			} else if n != 1 || got["h1"] != tc.want {
				t.Errorf("provenance = %v (%d records), want h1=%s: %s", got, n, tc.want, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"role"`) {
				t.Errorf("real facts were dropped: %s", rec.Body.String())
			}
		})
	}
}

// Dropping an upstream record is invisible in the response, so it is logged —
// once per request, not once per record.
func TestHandler_SuppressedUpstreamFactLoggedOncePerRequest(t *testing.T) {
	a := newFakeBackend(t,
		`[`+node("h1", "2026-07-20T00:00:00Z")+`,`+node("h2", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", defaultSourceFact, "old-h1", "")+`,`+fact("h2", defaultSourceFact, "old-h2", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	var buf bytes.Buffer
	srv := NewServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic), log.New(&buf, "", 0))

	doGet(t, srv.Handler(), factsPath, "")
	if n := strings.Count(buf.String(), defaultSourceFact); n != 1 {
		t.Fatalf("expected 1 log line naming the fact, got %d: %s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "dropped 2") {
		t.Errorf("log does not report the number dropped: %s", buf.String())
	}
}

// Disabled means untouched: an upstream fact of the configured name is served as
// the backend reported it.
func TestHandler_SourceDisabledKeepsUpstreamFact(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`,
		`[`+fact("h1", defaultSourceFact, "upstream-value", "")+`]`)
	b := newFakeBackend(t, `[]`, `[]`)
	cfg := testConfig(a.srv.URL, b.srv.URL, mergeStatic)
	cfg.SourceFactEnabled = false
	srv := newTestServer(cfg)

	rec := doGet(t, srv.Handler(), factsPath, "")
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != 1 || got["h1"] != "upstream-value" {
		t.Errorf("disabled injection altered the upstream fact: %s", rec.Body.String())
	}
}

// Suppression matches a record's own name field, so rows from a projection that
// filters on name without returning it are not self-identifying and pass
// through. Pinned as a known limit of the guarantee, and documented as one.
func TestHandler_ProjectionWithoutNameColumnCarriesUpstreamValue(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factsPath] = `[{"certname":"h1","value":"upstream-value"}]`
	var buf bytes.Buffer
	srv := NewServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic), log.New(&buf, "", 0))

	rec := doGet(t, srv.Handler(), factsPath,
		`["extract",["certname","value"],["=","name","`+defaultSourceFact+`"]]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); !strings.Contains(got, "upstream-value") {
		t.Errorf("unidentifiable row was dropped: %s", got)
	}
	if strings.Contains(buf.String(), "dropped") {
		t.Errorf("a row with no name field was counted as suppressed: %s", buf.String())
	}
	if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("gated projection gained %d synthetic records: %s", n, rec.Body.String())
	}
}

// A /facts aggregate is summed, and the summed row must gain no synthetic record
// and no stamp.
func TestHandler_SourceNotInjectedOnFactsAggregate(t *testing.T) {
	a := newFakeBackend(t, `[`+node("h1", "2026-07-20T00:00:00Z")+`]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.bodies[factsPath] = `[{"count":3}]`
	b.bodies[factsPath] = `[{"count":2}]`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := doGet(t, srv.Handler(), factsPath, `["extract",[["function","count"]]]`)
	if _, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact); n != 0 {
		t.Errorf("aggregate response gained %d synthetic records: %s", n, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), defaultSourceFact) {
		t.Errorf("aggregate rows were stamped: %s", rec.Body.String())
	}
	if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{5}) {
		t.Errorf("count = %v, want [5]", got)
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
		// openvoxdb hands each boolean operand back to user-node->plan-node, which
		// builds an extract node from it, so a nested extract can reshape the rows.
		{"extract under and", `["and",["=","certname","h1"],["extract",["certname"]]]`, true, false},
		{"extract under or", `["or",["extract",["certname"]],["=","certname","h1"]]`, true, false},
		{"extract under not", `["not",["extract",["certname"]]]`, true, false},
		{"extract nested two deep", `["and",["or",["extract",[["function","count"]]]]]`, true, false},
		{"extract under from", `["from","facts",["extract",["certname"]]]`, true, false},
		{"nested extract on nodes", `["and",["extract",["certname"]]]`, false, false},
		// An extract inside an `in` operand projects the subquery, not the response.
		{"extract under in stays injectable", `["in","certname",["extract",["certname"],["select_facts",["=","name","os"]]]]`, true, true},
		// `subquery` is rewritten to ["in" ... ["extract" ... ["select_x" ...]]]
		// before any plan node is built, so its operand is subquery-scoped too.
		{"subquery operand stays injectable", `["and",["=","certname","h1"],["subquery","facts",["extract",["certname"],["=","name","os"]]]]`, true, true},
		{"bare subquery stays injectable", `["subquery","facts",["extract",["certname"]]]`, true, true},
		{"extract under select_facts stays injectable", `["and",["select_facts",["extract",["certname"]]]]`, true, true},
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
	si := &sourceInjector{name: defaultSourceFact, inject: true}
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
	if si.injects() {
		t.Error("nil injector injects")
	}
	si.logSuppressed(log.New(io.Discard, "", 0))
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
	si := &sourceInjector{name: defaultSourceFact, inject: true}
	for _, raw := range []string{`"scalar"`, `[1,2]`, `null`} {
		if got := si.stamp(json.RawMessage(raw), "a"); string(got) != raw {
			t.Errorf("stamp(%s) = %s, want unchanged", raw, got)
		}
	}
}

// A gated injector suppresses without synthesising anything, on either endpoint.
func TestSourceInjector_GatedSuppressesButDoesNotInject(t *testing.T) {
	si := &sourceInjector{name: defaultSourceFact}
	if !si.claims(defaultSourceFact) {
		t.Error("gated injector does not claim its own fact name")
	}
	if si.factRecord("h1", "a", "production") != nil {
		t.Error("gated injector produced a record")
	}
	raw := json.RawMessage(`{"certname":"h1"}`)
	if got := si.stamp(raw, "a"); string(got) != string(raw) {
		t.Errorf("gated injector rewrote %s to %s", raw, got)
	}
}

// The disabled path must return the backends' records verbatim, source fact
// included.
func TestMergeFacts_DisabledIsUntouched(t *testing.T) {
	a := recs(t, "a", fact("h1", "role", "web-a", ""), fact("h1", defaultSourceFact, "upstream", ""))
	merged := mergeFacts([]backendResult{a}, nil, nil)

	got := factValues(t, merged)
	want := []string{"h1:role=web-a", "h1:" + defaultSourceFact + "=upstream"}
	if !slices.Equal(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// A gated query still gets the upstream record removed, and nothing added.
func TestMergeFacts_GatedSuppressesUpstream(t *testing.T) {
	a := recs(t, "a", fact("h1", "role", "web-a", ""), fact("h1", defaultSourceFact, "upstream", ""))
	si := &sourceInjector{name: defaultSourceFact}
	merged := mergeFacts([]backendResult{a}, nil, si)

	got := factValues(t, merged)
	if !slices.Equal(got, []string{"h1:role=web-a"}) {
		t.Errorf("merged = %v, want only the real fact", got)
	}
	if si.suppressed != 1 {
		t.Errorf("suppressed = %d, want 1", si.suppressed)
	}
}

func TestMergeFacts_SourceOrderedAfterOwnersFacts(t *testing.T) {
	a := recs(t, "a", fact("h1", "role", "web-a", ""), fact("h1", "kernel", "Linux", ""))
	b := recs(t, "b", fact("h1", "role", "web-b", ""))
	si := &sourceInjector{name: defaultSourceFact, hosts: map[string]string{"a": "a.example", "b": "b.example"}, inject: true}
	merged := mergeFacts([]backendResult{a, b}, nil, si)

	got := factValues(t, merged)
	want := []string{"h1:role=web-a", "h1:kernel=Linux", "h1:" + defaultSourceFact + "=a.example"}
	if !slices.Equal(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestMatchFact(t *testing.T) {
	f := factFields{Certname: "h1", Environment: "prod", Name: defaultSourceFact, Value: "a"}
	tests := []struct {
		query     string
		match, ok bool
	}{
		{`["=","name","` + defaultSourceFact + `"]`, true, true},
		{`["=","name","osfamily"]`, false, true},
		{`["=","value","a"]`, true, true},
		{`["=","value",1]`, false, true},
		{`["=","value",{"a":1}]`, false, true},
		{`["~","value","^a$"]`, true, true},
		{`["~","certname","^h"]`, true, true},
		{`["not",["=","value","a"]]`, false, true},
		{`["in","name",["array",["osfamily","` + defaultSourceFact + `"]]]`, true, true},
		{`["in","name",["array",["osfamily"]]]`, false, true},
		{`["and",["=","certname","h1"],["=","environment","prod"]]`, true, true},
		{`["and",["=","certname","h1"],["=","environment","dev"]]`, false, true},
		{`["or",["=","certname","h2"],["=","environment","prod"]]`, true, true},
		{`["and"]`, true, true},
		{`["or"]`, false, true},
		{`["and",["=","name","` + defaultSourceFact + `"],["null?","value",false]]`, false, false},
		{`["in","certname",["extract",["certname"],["select_nodes",["=","certname","h1"]]]]`, false, false},
		{`["=","fact_contents","x"]`, false, false},
		{`["~","value","("]`, false, false},
		{`["<","value","b"]`, false, false},
		{`["extract",["certname"],["=","name","x"]]`, false, false},
	}
	for _, tc := range tests {
		var ast []json.RawMessage
		if err := json.Unmarshal([]byte(tc.query), &ast); err != nil {
			t.Fatal(err)
		}
		if m, ok := matchFact(ast, f); m != tc.match || ok != tc.ok {
			t.Errorf("matchFact(%s) = %v, %v; want %v, %v", tc.query, m, ok, tc.match, tc.ok)
		}
	}
}

func TestSourceInjector_SourceQuery(t *testing.T) {
	si := &sourceInjector{name: defaultSourceFact}
	tests := []struct {
		query string
		want  bool
	}{
		{`["=","name","` + defaultSourceFact + `"]`, true},
		{`["and",["=","name","` + defaultSourceFact + `"]]`, true},
		{`["and",["or",["=","name","ipaddress"],["=","name","` + defaultSourceFact + `"]]]`, true},
		{`["and",["=","name","` + defaultSourceFact + `"],["not",["~","value","new"]]]`, true},
		{`["in","name",["array",["ipaddress","` + defaultSourceFact + `"]]]`, true},
		{`["~","name","^pdbmux"]`, true},
		{`["=","name","ipaddress"]`, false},
		{`["=","certname","h1"]`, false},
		{`["=","value","` + defaultSourceFact + `"]`, false},
		{`["and",["=","name","` + defaultSourceFact + `"],["in","certname",["extract",["certname"],["select_nodes",["=","certname","h1"]]]]]`, false},
		{``, false},
		{`not json`, false},
	}
	for _, tc := range tests {
		if got := si.sourceQuery(tc.query) != nil; got != tc.want {
			t.Errorf("sourceQuery(%s) = %v, want %v", tc.query, got, tc.want)
		}
	}
	var off *sourceInjector
	if off.sourceQuery(`["=","name","`+defaultSourceFact+`"]`) != nil {
		t.Error("disabled injector selected a query")
	}
}

// The value is the backend URL's hostname alone; the backend name is not used.
func TestSourceInjector_ValueIsBackendHostname(t *testing.T) {
	cfg := testConfig("http://puppetdb.puppet.svc.cluster.local:8080", "https://puppetdb.example.com/pdb", mergeStatic)
	srv := newTestServer(cfg)
	si := srv.newSourceInjector("", true)
	for backend, want := range map[string]string{"a": "puppetdb.puppet.svc.cluster.local", "b": "puppetdb.example.com"} {
		var rec factFields
		if err := json.Unmarshal(si.factRecord("h1", backend, ""), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Value != want {
			t.Errorf("fact value for %s = %q, want %q", backend, rec.Value, want)
		}
		if got := nodeSources(t, []byte(`[`+string(si.stamp([]byte(`{"certname":"h1"}`), backend))+`]`), defaultSourceFact); got["h1"] != want {
			t.Errorf("node stamp for %s = %v, want %q", backend, got, want)
		}
	}
}

func TestCertnameScope(t *testing.T) {
	tests := map[string]string{
		`["=","name","x"]`: ``,
		`["and",["=","name","x"],["=","environment","prod"]]`:                                    ``,
		`["or",["=","certname","h1"],["=","name","x"]]`:                                          ``,
		`["and",["not",["=","certname","h1"]],["=","name","x"]]`:                                 ``,
		`["and",["=","certname","h1"],["=","name","x"]]`:                                         `["=","certname","h1"]`,
		`["and",["~","certname","["],["=","name","x"]]`:                                          ``,
		`["and",["in","certname",["extract","certname",["=","a","b"]]],["=","name","x"]]`:        ``,
		`["and",["in","certname",["array",["h1","h2"]]],["~","certname","^h"],["=","name","x"]]`: `["and",["in","certname",["array",["h1","h2"]]],["~","certname","^h"]]`,
	}
	for in, want := range tests {
		var ast []json.RawMessage
		if err := json.Unmarshal([]byte(in), &ast); err != nil {
			t.Fatal(err)
		}
		if got := certnameScope(ast); got != want {
			t.Errorf("certnameScope(%s) = %q, want %q", in, got, want)
		}
	}
}
