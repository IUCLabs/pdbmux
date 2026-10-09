//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var allNodes = []string{nodeAlpha, nodeBeta, nodeGamma, nodeShared}

// The two backends hold disjoint nodes plus one they share; the merged view is
// the union, with the shared node appearing exactly once.
func TestNodesUnionAcrossBackends(t *testing.T) {
	resp := get(t, nodesPath, nil)
	rows := resp.rows(t)

	if got := e2eCertnames(rows); !equalStrings(got, allNodes) {
		t.Fatalf("merged /nodes certnames = %v, want %v", got, allNodes)
	}
	if len(rows) != len(allNodes) {
		t.Fatalf("merged /nodes returned %d records for %d nodes, so a shared node was not deduped", len(rows), len(allNodes))
	}
	// The deactivated node exists in backend A's database but must not surface.
	for _, row := range rows {
		if row["certname"] == nodeGone {
			t.Fatalf("deactivated node %s appeared in the merged /nodes response", nodeGone)
		}
	}
	if got := resp.header.Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
	}
}

// Facts from a node held by only one backend must survive the merge, which is
// what makes the merged view usable as a single PuppetDB.
func TestFactsUnionAcrossBackends(t *testing.T) {
	rows := get(t, factsPath, nil).rows(t)

	if got := e2eCertnames(rows); !equalStrings(got, allNodes) {
		t.Fatalf("merged /facts certnames = %v, want %v", got, allNodes)
	}
	for _, tc := range []struct{ certname, fact, want string }{
		{nodeAlpha, "only_a", "yes"},
		{nodeBeta, "only_b", "yes"},
	} {
		got, ok := factValue(rows, tc.certname, tc.fact)
		if !ok {
			t.Errorf("fact %s of %s is missing from the merged /facts response", tc.fact, tc.certname)
			continue
		}
		if got != tc.want {
			t.Errorf("fact %s of %s = %v, want %q", tc.fact, tc.certname, got, tc.want)
		}
	}
}

// The /facts/<name> path route carries the same records /facts does, so it takes
// the same merge: every node in the estate, each resolved to one backend.
func TestFactsByNamePathIsMerged(t *testing.T) {
	resp := get(t, factsPath+"/osfamily", nil)
	rows := resp.rows(t)

	if got := e2eCertnames(rows); !equalStrings(got, allNodes) {
		t.Fatalf("merged /facts/osfamily certnames = %v, want %v", got, allNodes)
	}
	if len(rows) != len(allNodes) {
		t.Fatalf("/facts/osfamily returned %d records for %d nodes, so a shared node was not deduped", len(rows), len(allNodes))
	}
	if got, _ := factValue(rows, nodeShared, "osfamily"); got != "Debian" {
		t.Errorf("osfamily of %s = %v, want Debian from the fresher backend", nodeShared, got)
	}
	if got := resp.header.Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
	}

	t.Run("the name constraint gates injection", func(t *testing.T) {
		for _, row := range rows {
			if row["name"] == defaultSourceFact {
				t.Fatalf("%s was injected into a /facts/<name> response: %v", defaultSourceFact, row)
			}
		}
	})

	t.Run("the value sub-route merges too", func(t *testing.T) {
		rows := get(t, factsPath+"/osfamily/Debian", nil).rows(t)
		want := []string{nodeBeta, nodeGamma, nodeShared}
		if got := e2eCertnames(rows); !equalStrings(got, want) {
			t.Errorf("/facts/osfamily/Debian certnames = %v, want %v", got, want)
		}
	})

	t.Run("a fact only one backend holds survives", func(t *testing.T) {
		rows := get(t, factsPath+"/only_b", nil).rows(t)
		if got, ok := factValue(rows, nodeBeta, "only_b"); !ok || got != "yes" {
			t.Errorf("/facts/only_b for %s = %v (present %v), want yes", nodeBeta, got, ok)
		}
	})
}

// /fact-names is a flat array of strings, so it needs its own union: the merged
// list is every backend's names, deduped and sorted.
func TestFactNamesAreMerged(t *testing.T) {
	resp := get(t, factNamesPath, nil)
	var got []string
	if err := json.Unmarshal(resp.body, &got); err != nil {
		t.Fatalf("/fact-names is not a flat string array: %v: %s", err, resp.body)
	}

	for _, name := range []string{"osfamily", "kernel", "role", "owner", "only_a", "only_b", "extra_b"} {
		if !containsString(got, name) {
			t.Errorf("merged /fact-names is missing %s: %v", name, got)
		}
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("merged /fact-names is not sorted ascending: %v", got)
	}
	seen := map[string]bool{}
	for _, name := range got {
		if seen[name] {
			t.Errorf("merged /fact-names lists %s twice: %v", name, got)
		}
		seen[name] = true
	}
	// The merged /facts response carries a record of the name pdbmux owns, so a
	// client discovering names here has to see it.
	if !seen[defaultSourceFact] {
		t.Errorf("merged /fact-names omits %s, which every merged /facts response carries: %v", defaultSourceFact, got)
	}
	if got := resp.header.Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
	}

	t.Run("paging and ordering are redone across the union", func(t *testing.T) {
		params := url.Values{
			"order_by":      {`[{"field":"name","order":"desc"}]`},
			"limit":         {"3"},
			"include_total": {"true"},
		}
		resp := get(t, factNamesPath, params)
		var page []string
		if err := json.Unmarshal(resp.body, &page); err != nil {
			t.Fatalf("decoding the paged /fact-names response: %v: %s", err, resp.body)
		}
		want := append([]string{}, got...)
		sort.Sort(sort.Reverse(sort.StringSlice(want)))
		if len(want) > 3 {
			want = want[:3]
		}
		if !equalStrings(page, want) {
			t.Errorf("descending /fact-names page = %v, want %v", page, want)
		}
		if n := resp.header.Get(recordsHeader); n != fmt.Sprint(len(got)) {
			t.Errorf("%s = %q, want the merged count %d", recordsHeader, n, len(got))
		}
	})

	// name is the only column the entity projects, so pdbmux rejects any other
	// order_by field the way the backends do.
	t.Run("an order_by on another field is rejected", func(t *testing.T) {
		params := url.Values{"order_by": {`[{"field":"bogus","order":"desc"}]`}}
		if r := rawGet(t, factNamesPath, params); r.status != http.StatusBadRequest {
			t.Errorf("merged /fact-names order_by bogus = HTTP %d, want 400: %s", r.status, r.body)
		}
		if r := h.a.queryStatus(context.Background(), t, factNamesPath, params); r != http.StatusBadRequest {
			t.Errorf("backend %s answered HTTP %d for the same order_by, so 400 is not what it does", h.a.name, r)
		}
	})
}

// A fact aggregate row carries no certname, so the per-certname merge would keep
// one backend's row and drop the other's; only adding the numbers is right.
func TestFactsByNamePathAggregatesAreSummed(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","count"]]]`

	for _, path := range []string{factsPath + "/kernel", factsPath + "/kernel/Linux"} {
		t.Run(path, func(t *testing.T) {
			wantA := backendCount(ctx, t, h.a, path, q)
			wantB := backendCount(ctx, t, h.b, path, q)
			if wantA == wantB {
				t.Fatalf("the fixture gives both backends %d records on %s, so a sum is indistinguishable from one backend's number", wantA, path)
			}

			resp := get(t, path, query(q))
			if got := countOf(t, resp.rows(t)); got != wantA+wantB {
				t.Fatalf("%s count = %d, want %d (%s=%d + %s=%d)", path, got, wantA+wantB, h.a.name, wantA, h.b.name, wantB)
			}
			if got := resp.header.Get(backendsHeader); got != "2/2" {
				t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
			}
		})
	}

	t.Run("grouped counts are summed per key", func(t *testing.T) {
		const grouped = `["extract",[["function","count"],"value"],["group_by","value"]]`
		path := factsPath + "/osfamily"
		want := map[string]int{}
		for _, b := range []*backend{h.a, h.b} {
			for value, n := range countsByValue(t, b.query(ctx, t, path, query(grouped))) {
				want[value] += n
			}
		}
		got := countsByValue(t, get(t, path, query(grouped)).rows(t))
		if !equalCounts(got, want) {
			t.Errorf("grouped %s counts = %v, want %v", path, got, want)
		}
	})

	t.Run("a query with no function column still merges by certname", func(t *testing.T) {
		rows := get(t, factsPath+"/kernel", nil).rows(t)
		if got := e2eCertnames(rows); !equalStrings(got, allNodes) {
			t.Errorf("/facts/kernel certnames = %v, want %v", got, allNodes)
		}
		if len(rows) != len(allNodes) {
			t.Errorf("/facts/kernel returned %d records for %d nodes, so a shared node was not deduped", len(rows), len(allNodes))
		}
	})
}

// countsByValue reads a ["function","count"] + group_by "value" result set.
func countsByValue(t *testing.T, rows []map[string]any) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, row := range rows {
		value, ok := row["value"].(string)
		if !ok {
			t.Fatalf("grouped aggregate row has no value: %v", row)
		}
		n, ok := row["count"].(float64)
		if !ok {
			t.Fatalf("grouped aggregate row has no numeric count: %v", row)
		}
		out[value] = int(n)
	}
	return out
}

func equalCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// The shared node's two copies disagree on every fact value; the backend holding
// its newer report_timestamp has to win both endpoints.
func TestSharedNodeResolvesToTheFresherBackend(t *testing.T) {
	ctx := context.Background()

	// The losing value really is present upstream, so the assertions below are
	// about the merge and not about missing data.
	aFacts := h.a.query(ctx, t, factsPath, query(`["=","certname","`+nodeShared+`"]`))
	if got, _ := factValue(aFacts, nodeShared, "owner"); got != backendAName {
		t.Fatalf("backend %s holds owner=%v for %s, want %q", h.a.name, got, nodeShared, backendAName)
	}
	bFacts := h.b.query(ctx, t, factsPath, query(`["=","certname","`+nodeShared+`"]`))
	if got, _ := factValue(bFacts, nodeShared, "owner"); got != backendBName {
		t.Fatalf("backend %s holds owner=%v for %s, want %q", h.b.name, got, nodeShared, backendBName)
	}

	node := nodeRow(t, get(t, nodesPath, nil).rows(t), nodeShared)
	if got := node["report_timestamp"]; got != tsSharedOnB {
		t.Errorf("merged /nodes report_timestamp for %s = %v, want the fresher %q", nodeShared, got, tsSharedOnB)
	}
	if got := node[defaultSourceFact]; got != backendBHost {
		t.Errorf("merged /nodes %s for %s = %v, want %q", defaultSourceFact, nodeShared, got, backendBHost)
	}

	rows := get(t, factsPath, query(`["=","certname","`+nodeShared+`"]`)).rows(t)
	for _, tc := range []struct {
		fact string
		want any
	}{
		{"owner", backendBName},
		{"osfamily", "Debian"}, // backend A holds RedHat for the same node
	} {
		got, ok := factValue(rows, nodeShared, tc.fact)
		if !ok {
			t.Errorf("fact %s of %s is missing from the merged /facts response", tc.fact, nodeShared)
			continue
		}
		if got != tc.want {
			t.Errorf("merged fact %s of %s = %v, want %v from the fresher backend", tc.fact, nodeShared, got, tc.want)
		}
	}

	// Only one backend's facts are kept, so no fact name may appear twice.
	seen := map[string]int{}
	for _, row := range rows {
		if row["certname"] == nodeShared {
			seen[row["name"].(string)]++
		}
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("fact %s of %s appears %d times; both backends' copies were kept", name, nodeShared, n)
		}
	}
}

// A count row carries no certname, so it can only be right if the backends'
// numbers are added rather than deduped or taken from one.
func TestNodesAggregatesAreSummed(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","count"]]]`

	wantA := backendCount(ctx, t, h.a, nodesPath, q)
	wantB := backendCount(ctx, t, h.b, nodesPath, q)
	if wantA == wantB {
		t.Fatalf("the fixture gives both backends %d nodes, so a sum is indistinguishable from one backend's number", wantA)
	}

	got := countOf(t, get(t, nodesPath, query(q)).rows(t))
	if got != wantA+wantB {
		t.Fatalf("/nodes count = %d, want %d (%s=%d + %s=%d)", got, wantA+wantB, h.a.name, wantA, h.b.name, wantB)
	}

	const grouped = `["extract",[["function","count"],"facts_environment"],["group_by","facts_environment"]]`
	rows := get(t, nodesPath, query(grouped)).rows(t)
	if len(rows) != 1 {
		t.Fatalf("grouped /nodes count returned %d rows, want 1 (every node is in one environment): %v", len(rows), rows)
	}
	if rows[0]["facts_environment"] != "production" {
		t.Errorf("grouped /nodes count environment = %v, want production", rows[0]["facts_environment"])
	}
	if got := countOf(t, rows); got != wantA+wantB {
		t.Errorf("grouped /nodes count = %d, want %d", got, wantA+wantB)
	}
}

func TestResourcesAggregatesAreSummed(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","count"]]]`

	wantA := backendCount(ctx, t, h.a, resourcesPath, q)
	wantB := backendCount(ctx, t, h.b, resourcesPath, q)
	got := countOf(t, get(t, resourcesPath, query(q)).rows(t))
	if got != wantA+wantB {
		t.Fatalf("/resources count = %d, want %d (%s=%d + %s=%d)", got, wantA+wantB, h.a.name, wantA, h.b.name, wantB)
	}
}

// oneAggregateRow reads the single row a whole-estate aggregate returns.
func oneAggregateRow(t *testing.T, rows []map[string]any) map[string]any {
	t.Helper()
	if len(rows) != 1 {
		t.Fatalf("want exactly one aggregate row, got %d: %v", len(rows), rows)
	}
	return rows[0]
}

// aggregateNumber reads one numeric aggregate column, which openvoxdb nulls when
// the backend matched nothing.
func aggregateNumber(t *testing.T, row map[string]any, column string) float64 {
	t.Helper()
	n, ok := row[column].(float64)
	if !ok {
		t.Fatalf("aggregate row has no numeric %s: %v", column, row)
	}
	return n
}

// backendAggregate runs a whole-estate aggregate against one backend directly.
func backendAggregate(ctx context.Context, t *testing.T, b *backend, path, q string) map[string]any {
	t.Helper()
	return oneAggregateRow(t, b.query(ctx, t, path, query(q)))
}

// min and max are extremes, not sums: the merged answer has to be the smallest
// and largest the backends reported, both of which the fixture makes distinct.
func TestResourcesMinMaxAreExtremes(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","min","line"],["function","max","line"]]]`

	a := backendAggregate(ctx, t, h.a, resourcesPath, q)
	b := backendAggregate(ctx, t, h.b, resourcesPath, q)
	minA, maxA := aggregateNumber(t, a, "min"), aggregateNumber(t, a, "max")
	minB, maxB := aggregateNumber(t, b, "min"), aggregateNumber(t, b, "max")
	if minA == minB || maxA == maxB {
		t.Fatalf("the fixture gives both backends the same extremes (min %v/%v, max %v/%v)", minA, minB, maxA, maxB)
	}

	got := oneAggregateRow(t, get(t, resourcesPath, query(q)).rows(t))
	if wantMin := math.Min(minA, minB); aggregateNumber(t, got, "min") != wantMin {
		t.Errorf("/resources min = %v, want %v (%s=%v, %s=%v)", got["min"], wantMin, h.a.name, minA, h.b.name, minB)
	}
	wantMax := math.Max(maxA, maxB)
	if gotMax := aggregateNumber(t, got, "max"); gotMax != wantMax {
		t.Errorf("/resources max = %v, want %v (%s=%v, %s=%v)", gotMax, wantMax, h.a.name, maxA, h.b.name, maxB)
	} else if gotMax == maxA+maxB {
		t.Errorf("/resources max = %v, which is the sum of the backends' maxima", gotMax)
	}
}

// openvoxdb allows min/max on text columns, where the merged answer used to be
// whichever backend happened to answer first. The fixture puts the estate's
// smallest resource title in one backend and its largest in the other, so a
// first-wins merge gets one of the two wrong whichever way round it reads them.
func TestResourcesMinMaxOnATextColumn(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","min","title"],["function","max","title"]],["=","type","File"]]`

	a := backendAggregate(ctx, t, h.a, resourcesPath, q)
	b := backendAggregate(ctx, t, h.b, resourcesPath, q)
	minA, minB := aggregateString(t, a, "min"), aggregateString(t, b, "min")
	maxA, maxB := aggregateString(t, a, "max"), aggregateString(t, b, "max")
	if minA == minB || maxA == maxB {
		t.Fatalf("the fixture gives both backends the same extremes (min %q/%q, max %q/%q)", minA, minB, maxA, maxB)
	}

	got := oneAggregateRow(t, get(t, resourcesPath, query(q)).rows(t))
	if want := min(minA, minB); got["min"] != want {
		t.Errorf("/resources min(title) = %v, want %q (%s=%q, %s=%q)", got["min"], want, h.a.name, minA, h.b.name, minB)
	}
	if want := max(maxA, maxB); got["max"] != want {
		t.Errorf("/resources max(title) = %v, want %q (%s=%q, %s=%q)", got["max"], want, h.a.name, maxA, h.b.name, maxB)
	}
}

func aggregateString(t *testing.T, row map[string]any, column string) string {
	t.Helper()
	s, ok := row[column].(string)
	if !ok {
		t.Fatalf("aggregate row has no string %s: %v", column, row)
	}
	return s
}

// avg is decomposed into an upstream sum and count, so the merged answer is the
// true weighted average of the estate rather than an average of averages.
func TestResourcesAvgIsWeighted(t *testing.T) {
	ctx := context.Background()
	const parts = `["extract",[["function","sum","line"],["function","count","line"]]]`

	a := backendAggregate(ctx, t, h.a, resourcesPath, parts)
	b := backendAggregate(ctx, t, h.b, resourcesPath, parts)
	sumA, countA := aggregateNumber(t, a, "sum"), aggregateNumber(t, a, "count")
	sumB, countB := aggregateNumber(t, b, "sum"), aggregateNumber(t, b, "count")
	if countA == countB {
		t.Fatalf("the fixture gives both backends %v rows, so a weighted average is indistinguishable from a plain one", countA)
	}
	want := (sumA + sumB) / (countA + countB)

	const q = `["extract",[["function","avg","line"]]]`
	got := aggregateNumber(t, oneAggregateRow(t, get(t, resourcesPath, query(q)).rows(t)), "avg")
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("/resources avg(line) = %v, want %v ((%v+%v)/(%v+%v))", got, want, sumA, sumB, countA, countB)
	}

	avgA := aggregateNumber(t, backendAggregate(ctx, t, h.a, resourcesPath, q), "avg")
	avgB := aggregateNumber(t, backendAggregate(ctx, t, h.b, resourcesPath, q), "avg")
	if math.Abs(got-(avgA+avgB)) < 1e-9 {
		t.Errorf("/resources avg(line) = %v, which is the sum of the backends' averages", got)
	}
	if math.Abs(got-(avgA+avgB)/2) < 1e-9 {
		t.Errorf("/resources avg(line) = %v, which is an unweighted average of averages", got)
	}
}

func TestResourcesAvgWithGroupBy(t *testing.T) {
	ctx := context.Background()
	const parts = `["extract",[["function","sum","line"],["function","count","line"],"type"],["group_by","type"]]`
	const q = `["extract",[["function","avg","line"],"type"],["group_by","type"]]`

	sums, counts := map[string]float64{}, map[string]float64{}
	for _, b := range []*backend{h.a, h.b} {
		for _, row := range b.query(ctx, t, resourcesPath, query(parts)) {
			typ, ok := row["type"].(string)
			if !ok {
				t.Fatalf("grouped row has no type: %v", row)
			}
			if n, ok := row["sum"].(float64); ok {
				sums[typ] += n
				counts[typ] += aggregateNumber(t, row, "count")
			}
		}
	}
	if len(sums) == 0 {
		t.Fatal("no grouped rows carried a sum, so the weighted average is untested")
	}

	for _, row := range get(t, resourcesPath, query(q)).rows(t) {
		typ, ok := row["type"].(string)
		if !ok {
			t.Fatalf("grouped row has no type: %v", row)
		}
		if counts[typ] == 0 {
			if row["avg"] != nil {
				t.Errorf("avg(line) for %s = %v, want null over no rows", typ, row["avg"])
			}
			continue
		}
		want := sums[typ] / counts[typ]
		if got := aggregateNumber(t, row, "avg"); math.Abs(got-want) > 1e-9 {
			t.Errorf("avg(line) for %s = %v, want %v", typ, got, want)
		}
	}
}

// A query pdbmux cannot merge is refused outright rather than answered with a
// plausible wrong number.
func TestUnmergeableAggregateIsRefused(t *testing.T) {
	resp := rawGet(t, resourcesPath, query(`["extract",[["function","avg","line"],["function","count"]]]`))
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", resp.status, resp.body)
	}
	if !strings.Contains(string(resp.body), "avg") {
		t.Errorf("refusal %q does not name the limitation", resp.body)
	}
}

// openvoxdb names every extract column after its function and aliases a repeat
// as "<name>_2", which pdbmux's spec never learns about: it would be neither a
// grouping key nor a folded aggregate, so the first backend's value would
// freeze into the merged row. The backend answers such a query, which is
// exactly why pdbmux has to refuse it.
func TestRepeatedFunctionColumnIsRefused(t *testing.T) {
	const q = `["extract",[["function","count","certname"],["function","count","catalog_environment"]]]`

	resp := rawGet(t, nodesPath, query(q))
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", resp.status, resp.body)
	}
	if !strings.Contains(string(resp.body), "count") {
		t.Errorf("refusal %q does not name the clashing column", resp.body)
	}

	rows := h.a.query(context.Background(), t, nodesPath, query(q))
	if len(rows) != 1 {
		t.Fatalf("backend %s returned %d rows for the repeated projection, want 1", h.a.name, len(rows))
	}
	if _, ok := rows[0]["count_2"]; !ok {
		t.Errorf("backend %s row = %v, want the aliased count_2 column the refusal exists for", h.a.name, rows[0])
	}
}

// to_string is a scalar expression, so it yields one row per record: alone it
// must not collapse the estate into a single row, and with a companion count it
// groups.
func TestReportsToString(t *testing.T) {
	ctx := context.Background()
	const bare = `["extract",[["function","to_string","producer_timestamp","FMDAY"]]]`

	wantRows := len(h.a.query(ctx, t, reportsPath, query(bare))) + len(h.b.query(ctx, t, reportsPath, query(bare)))
	if wantRows < 2 {
		t.Fatalf("the fixture yields %d to_string rows, so a collapse would be invisible", wantRows)
	}
	rows := get(t, reportsPath, query(bare)).rows(t)
	if len(rows) != wantRows {
		t.Errorf("/reports to_string returned %d rows, want %d (every backend's rows)", len(rows), wantRows)
	}
	for _, row := range rows {
		if _, ok := row["to_string"].(string); !ok {
			t.Fatalf("to_string row has no string column: %v", row)
		}
	}

	const grouped = `["extract",[["function","to_string","producer_timestamp","FMDAY"],["function","count"]],["group_by",["function","to_string","producer_timestamp","FMDAY"]]]`
	want := map[string]float64{}
	for _, b := range []*backend{h.a, h.b} {
		for _, row := range b.query(ctx, t, reportsPath, query(grouped)) {
			want[row["to_string"].(string)] += aggregateNumber(t, row, "count")
		}
	}
	got := map[string]float64{}
	for _, row := range get(t, reportsPath, query(grouped)).rows(t) {
		got[row["to_string"].(string)] += aggregateNumber(t, row, "count")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grouped /reports to_string counts = %v, want %v", got, want)
	}
}

// A fact count row carries no certname, so the per-certname fact merge would
// keep one backend's rows and drop the other's; only adding the numbers is right.
func TestFactsAggregatesAreSummed(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","count"]]]`

	wantA := backendCount(ctx, t, h.a, factsPath, q)
	wantB := backendCount(ctx, t, h.b, factsPath, q)
	if wantA == wantB {
		t.Fatalf("the fixture gives both backends %d facts, so a sum is indistinguishable from one backend's number", wantA)
	}

	resp := get(t, factsPath, query(q))
	if got := countOf(t, resp.rows(t)); got != wantA+wantB {
		t.Fatalf("/facts count = %d, want %d (%s=%d + %s=%d)", got, wantA+wantB, h.a.name, wantA, h.b.name, wantB)
	}
	if got := resp.header.Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
	}

	const grouped = `["extract",[["function","count"],"name"],["group_by","name"]]`
	want := map[string]int{}
	for _, b := range []*backend{h.a, h.b} {
		for name, n := range countsByName(t, b.query(ctx, t, factsPath, query(grouped))) {
			want[name] += n
		}
	}
	got := countsByName(t, get(t, factsPath, query(grouped)).rows(t))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grouped /facts counts = %v, want %v", got, want)
	}
	// The aggregate path must not inject provenance, which a group_by on name would expose.
	if n, ok := got[defaultSourceFact]; ok {
		t.Errorf("grouped /facts counts include %d synthetic %s rows", n, defaultSourceFact)
	}
}

// countsByName reads a ["function","count"] + group_by "name" result set.
func countsByName(t *testing.T, rows []map[string]any) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, row := range rows {
		name, ok := row["name"].(string)
		if !ok {
			t.Fatalf("grouped aggregate row has no name: %v", row)
		}
		n, ok := row["count"].(float64)
		if !ok {
			t.Fatalf("grouped aggregate row has no numeric count: %v", row)
		}
		out[name] = int(n)
	}
	return out
}

// An events count row is an aggregate, not an event, so the union's
// verbatim-record dedupe would fold two backends' equal counts into one number.
func TestEventsAggregatesAreSummed(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","count"]]]`

	wantA := backendCount(ctx, t, h.a, eventsPath, q)
	wantB := backendCount(ctx, t, h.b, eventsPath, q)
	if wantA == 0 || wantB == 0 {
		t.Fatalf("the fixture gives %s %d and %s %d events, so a sum proves nothing", h.a.name, wantA, h.b.name, wantB)
	}

	resp := get(t, eventsPath, query(q))
	if got := countOf(t, resp.rows(t)); got != wantA+wantB {
		t.Fatalf("/events count = %d, want %d (%s=%d + %s=%d)", got, wantA+wantB, h.a.name, wantA, h.b.name, wantB)
	}
	if got := resp.header.Get(backendsHeader); got != "2/2" {
		t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
	}

	t.Run("grouped counts are summed per key", func(t *testing.T) {
		const grouped = `["extract",[["function","count"],"certname"],["group_by","certname"]]`
		want := map[string]int{}
		for _, b := range []*backend{h.a, h.b} {
			for cn, n := range countsByCertname(t, b.query(ctx, t, eventsPath, query(grouped))) {
				want[cn] += n
			}
		}
		// shared reports to both backends, so its row is the one a dedupe would
		// leave frozen at a single backend's number.
		if want[nodeShared] < 2 {
			t.Fatalf("%s has %d events across the estate, so its merged row cannot show a sum", nodeShared, want[nodeShared])
		}
		got := countsByCertname(t, get(t, eventsPath, query(grouped)).rows(t))
		if !reflect.DeepEqual(got, want) {
			t.Errorf("grouped /events counts = %v, want %v", got, want)
		}
	})

	t.Run("a query with no function column stays on the union", func(t *testing.T) {
		rows := get(t, eventsPath, nil).rows(t)
		if len(rows) != wantA+wantB {
			t.Fatalf("/events returned %d records, want %d: the union dropped or duplicated events", len(rows), wantA+wantB)
		}
		for _, row := range rows {
			if _, ok := row["resource_title"].(string); !ok {
				t.Fatalf("/events record is not an event: %v", row)
			}
		}
	})
}

// distinct_resources moves /events onto openvoxdb's legacy compiler, which has
// no function or group_by: both backends fail identically, so without a refusal
// a client mistake reaches the caller as an outage-shaped 502.
func TestEventsAggregateWithDistinctResourcesIsRefused(t *testing.T) {
	ctx := context.Background()
	distinct := func(q string) url.Values {
		v := url.Values{
			"distinct_resources":  {"true"},
			"distinct_start_time": {fixtureTime(-24 * time.Hour)},
			"distinct_end_time":   {fixtureTime(time.Hour)},
		}
		if q != "" {
			v.Set("query", q)
		}
		return v
	}

	params := distinct(`["extract",[["function","count"]]]`)
	resp := rawGet(t, eventsPath, params)
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", resp.status, resp.body)
	}
	if !strings.Contains(string(resp.body), "distinct_resources") {
		t.Errorf("refusal %q does not name the incompatibility", resp.body)
	}
	if got := h.a.queryStatus(ctx, t, eventsPath, params); got == http.StatusOK {
		t.Errorf("backend %s answered the aggregate with 200, so the refusal is unnecessary", h.a.name)
	}

	t.Run("a plain distinct_resources query is still served", func(t *testing.T) {
		params := distinct("")
		want := len(h.a.query(ctx, t, eventsPath, params)) + len(h.b.query(ctx, t, eventsPath, params))
		if want == 0 {
			t.Fatal("the fixture yields no distinct-resources events, so the union proves nothing")
		}
		rows := get(t, eventsPath, params).rows(t)
		if len(rows) != want {
			t.Fatalf("/events with distinct_resources returned %d records, want %d", len(rows), want)
		}
		for _, row := range rows {
			if _, ok := row["resource_title"].(string); !ok {
				t.Fatalf("/events record is not an event: %v", row)
			}
		}
	})
}

// countsByCertname reads a ["function","count"] + group_by "certname" result set.
func countsByCertname(t *testing.T, rows []map[string]any) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, row := range rows {
		certname, ok := row["certname"].(string)
		if !ok {
			t.Fatalf("grouped aggregate row has no certname: %v", row)
		}
		out[certname] = int(aggregateNumber(t, row, "count"))
	}
	return out
}

// The provenance fact must name the backend whose data won, and must be absent
// from the query shapes it would corrupt.
func TestSourceFactInjectionAndGating(t *testing.T) {
	t.Run("facts carry one source record per node", func(t *testing.T) {
		rows := get(t, factsPath, nil).rows(t)
		want := map[string]string{
			nodeAlpha:  backendAHost,
			nodeBeta:   backendBHost,
			nodeGamma:  backendBHost,
			nodeShared: backendBHost, // won on freshness, not on configured order
		}
		got := map[string]int{}
		for _, row := range rows {
			if row["name"] != defaultSourceFact {
				continue
			}
			cn, _ := row["certname"].(string)
			got[cn]++
			if row["value"] != want[cn] {
				t.Errorf("%s for %s = %v, want %q", defaultSourceFact, cn, row["value"], want[cn])
			}
			if _, ok := row["environment"]; !ok {
				t.Errorf("%s record for %s has no environment key; clients index all four", defaultSourceFact, cn)
			}
		}
		for cn := range want {
			if got[cn] != 1 {
				t.Errorf("%s appears %d times for %s, want exactly 1", defaultSourceFact, got[cn], cn)
			}
		}
	})

	t.Run("nodes carry a source key", func(t *testing.T) {
		for _, row := range get(t, nodesPath, nil).rows(t) {
			if _, ok := row[defaultSourceFact]; !ok {
				t.Errorf("/nodes record for %v has no %s key", row["certname"], defaultSourceFact)
			}
		}
	})

	t.Run("no injection on a top-level extract", func(t *testing.T) {
		rows := get(t, factsPath, query(`["extract",["certname","name","value"]]`)).rows(t)
		if len(rows) == 0 {
			t.Fatal("the extract projection returned nothing, so the gate is untested")
		}
		for _, row := range rows {
			if row["name"] == defaultSourceFact {
				t.Fatalf("%s was injected into an extract projection: %v", defaultSourceFact, row)
			}
		}
	})

	t.Run("no injection on a name-constrained facts query", func(t *testing.T) {
		rows := get(t, factsPath, query(`["=","name","kernel"]`)).rows(t)
		if len(rows) != len(allNodes) {
			t.Fatalf("kernel query returned %d records, want one per node (%d): %v", len(rows), len(allNodes), rows)
		}
		for _, row := range rows {
			if row["name"] == defaultSourceFact {
				t.Fatalf("%s was injected into a name-constrained query: %v", defaultSourceFact, row)
			}
		}
	})

	t.Run("injection survives a certname filter", func(t *testing.T) {
		rows := get(t, factsPath, query(`["=","certname","`+nodeAlpha+`"]`)).rows(t)
		if _, ok := factValue(rows, nodeAlpha, defaultSourceFact); !ok {
			t.Fatalf("%s is missing from a certname-filtered query, which is not a gated shape", defaultSourceFact)
		}
	})
}

// /fact-names advertises the fact, so its drilldown has to answer with the same
// records /facts carries rather than the empty set the backends hold.
func TestSourceFactDrilldown(t *testing.T) {
	want := map[string]string{
		nodeAlpha:  backendAHost,
		nodeBeta:   backendBHost,
		nodeGamma:  backendBHost,
		nodeShared: backendBHost, // won on freshness, not on configured order
	}
	path := factsPath + "/" + defaultSourceFact

	t.Run("one record per node, attributed as /facts attributes it", func(t *testing.T) {
		resp := get(t, path, nil)
		rows := resp.rows(t)
		if got := e2eCertnames(rows); !equalStrings(got, allNodes) {
			t.Fatalf("%s certnames = %v, want %v", path, got, allNodes)
		}
		if len(rows) != len(allNodes) {
			t.Fatalf("%s returned %d records for %d nodes", path, len(rows), len(allNodes))
		}
		unfiltered := get(t, factsPath, nil).rows(t)
		for _, row := range rows {
			cn, _ := row["certname"].(string)
			if row["name"] != defaultSourceFact {
				t.Errorf("%s returned a record named %v", path, row["name"])
			}
			if row["value"] != want[cn] {
				t.Errorf("%s for %s = %v, want %q", path, cn, row["value"], want[cn])
			}
			// The drilldown must not disagree with the response it stands for.
			if got, ok := factValue(unfiltered, cn, defaultSourceFact); !ok || got != row["value"] {
				t.Errorf("%s for %s = %v, want the %s value %v", path, cn, row["value"], factsPath, got)
			}
			if _, ok := row["environment"]; !ok {
				t.Errorf("%s record for %s has no environment key", path, cn)
			}
		}
		if got := resp.header.Get(backendsHeader); got != "2/2" {
			t.Errorf("%s = %q, want %q", backendsHeader, got, "2/2")
		}
	})

	t.Run("the value sub-route filters by owning backend", func(t *testing.T) {
		for _, tc := range []struct {
			value string
			want  []string
		}{
			{backendAHost, []string{nodeAlpha}},
			{backendBHost, []string{nodeBeta, nodeGamma, nodeShared}},
			{"nosuchbackend", nil},
		} {
			rows := get(t, path+"/"+tc.value, nil).rows(t)
			if got := e2eCertnames(rows); !equalStrings(got, tc.want) {
				t.Errorf("%s/%s certnames = %v, want %v", path, tc.value, got, tc.want)
			}
		}
	})

	t.Run("a certname query narrows the drilldown", func(t *testing.T) {
		rows := get(t, path, query(`["=","certname","`+nodeAlpha+`"]`)).rows(t)
		if got := e2eCertnames(rows); !equalStrings(got, []string{nodeAlpha}) {
			t.Errorf("certname-filtered %s = %v, want only %s", path, got, nodeAlpha)
		}
	})

	// An aggregate carries no certname to attribute, so it stays summed.
	t.Run("an aggregate is still summed", func(t *testing.T) {
		ctx := context.Background()
		const q = `["extract",[["function","count"]]]`
		wantA := backendCount(ctx, t, h.a, path, q)
		wantB := backendCount(ctx, t, h.b, path, q)
		if got := countOf(t, get(t, path, query(q)).rows(t)); got != wantA+wantB {
			t.Errorf("%s count = %d, want %d (%s=%d + %s=%d)", path, got, wantA+wantB, h.a.name, wantA, h.b.name, wantB)
		}
	})
}

// X-Backends has to report what the response was actually built from, not what
// is configured.
func TestBackendsHeaderReportsContributors(t *testing.T) {
	for _, path := range []string{nodesPath, factsPath, reportsPath, eventsPath} {
		if got := get(t, path, nil).header.Get(backendsHeader); got != "2/2" {
			t.Errorf("%s on %s = %q, want %q", backendsHeader, path, got, "2/2")
		}
	}
}

// Reports and events are immutable history, so a node reporting to two backends
// keeps both records rather than being resolved to one.
func TestReportsAndEventsUnion(t *testing.T) {
	rows := get(t, reportsPath, nil).rows(t)

	// Deactivating a node retires it from /nodes and /facts but not its stored
	// runs, so the merged history still carries them.
	wantReported := append(append([]string{}, allNodes...), nodeGone)
	sort.Strings(wantReported)
	if got := e2eCertnames(rows); !equalStrings(got, wantReported) {
		t.Fatalf("merged /reports certnames = %v, want %v", got, wantReported)
	}
	perNode := map[string]int{}
	hashes := map[string]bool{}
	for _, row := range rows {
		cn, _ := row["certname"].(string)
		perNode[cn]++
		hash, _ := row["hash"].(string)
		if hash == "" {
			t.Fatalf("/reports record for %s has no hash: %v", cn, row)
		}
		if hashes[hash] {
			t.Errorf("report hash %s appeared twice; the union did not dedupe", hash)
		}
		hashes[hash] = true
	}
	if perNode[nodeShared] != 2 {
		t.Errorf("%s has %d reports, want 2 — one from each backend, since reports are history and not deduped by node",
			nodeShared, perNode[nodeShared])
	}

	t.Run("ordering is redone across the union", func(t *testing.T) {
		params := url.Values{
			"order_by":      {`[{"field":"end_time","order":"desc"}]`},
			"include_total": {"true"},
		}
		resp := get(t, reportsPath, params)
		ordered := resp.rows(t)
		if len(ordered) != len(rows) {
			t.Fatalf("ordered /reports returned %d records, want %d", len(ordered), len(rows))
		}
		// The newest report in the estate lives in backend B, so a response ordered
		// only within one backend's slice would not start here.
		if got := ordered[0]["end_time"]; got != tsSharedOnB {
			t.Errorf("newest merged report end_time = %v, want %q", got, tsSharedOnB)
		}
		for i := 1; i < len(ordered); i++ {
			if ordered[i-1]["end_time"].(string) < ordered[i]["end_time"].(string) {
				t.Fatalf("merged /reports is not sorted descending at index %d: %v", i, ordered)
			}
		}
		if got := resp.header.Get(recordsHeader); got == "" {
			t.Errorf("include_total=true set no %s header", recordsHeader)
		}
	})

	t.Run("events union", func(t *testing.T) {
		events := get(t, eventsPath, nil).rows(t)
		if got := e2eCertnames(events); !equalStrings(got, wantReported) {
			t.Fatalf("merged /events certnames = %v, want %v", got, wantReported)
		}
		for _, cn := range allNodes {
			want := eventMessage(cn)
			found := false
			for _, ev := range events {
				if ev["certname"] == cn && ev["message"] == want {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("no event with message %q for %s in the merged /events response", want, cn)
			}
		}
	})

	t.Run("report sub-resources resolve to the holding backend", func(t *testing.T) {
		ctx := context.Background()
		// A hash only backend B holds: the first backend answers 404, so serving it
		// at all proves every backend is consulted.
		bReports := h.b.query(ctx, t, reportsPath, query(`["=","certname","`+nodeBeta+`"]`))
		if len(bReports) == 0 {
			t.Fatalf("backend %s holds no report for %s", h.b.name, nodeBeta)
		}
		hash := bReports[0]["hash"].(string)

		logs := get(t, reportsPath+"/"+hash+"/logs", nil).rows(t)
		if len(logs) == 0 {
			t.Fatalf("no logs served for report %s, which only backend %s holds", hash, h.b.name)
		}
		if got := logs[0]["message"]; got != "e2e run for "+nodeBeta {
			t.Errorf("log message for %s = %v, want the fixture's", nodeBeta, got)
		}
	})
}

// Event counts are per-subject aggregates, so the shared node's counts have to
// be added across the backends that each saw one of its runs.
func TestEventCountsAreSummed(t *testing.T) {
	params := url.Values{
		"query":        {`["=","latest_report?",true]`},
		"summarize_by": {"certname"},
	}
	rows := get(t, eventCountsPath, params).rows(t)

	got := map[string]float64{}
	for _, row := range rows {
		subject, ok := row["subject"].(map[string]any)
		if !ok {
			t.Fatalf("event-counts row has no subject object: %v", row)
		}
		title, _ := subject["title"].(string)
		successes, _ := row["successes"].(float64)
		got[title] = successes
	}
	if got[nodeShared] != 2 {
		t.Errorf("successes for %s = %v, want 2 — one from each backend, summed", nodeShared, got[nodeShared])
	}
	for _, cn := range []string{nodeAlpha, nodeBeta, nodeGamma} {
		if got[cn] != 1 {
			t.Errorf("successes for %s = %v, want 1", cn, got[cn])
		}
	}
}

// The meta endpoints back a client's feature detection, so they have to answer
// through the merge rather than 404.
func TestMetaEndpoints(t *testing.T) {
	resp := get(t, metaVersionPath, nil)
	var version struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(resp.body, &version); err != nil || version.Version == "" {
		t.Fatalf("%s returned %s", metaVersionPath, resp.body)
	}

	resp = get(t, metaServerTimePath, nil)
	var serverTime struct {
		ServerTime string `json:"server_time"`
	}
	if err := json.Unmarshal(resp.body, &serverTime); err != nil || serverTime.ServerTime == "" {
		t.Fatalf("%s returned %s", metaServerTimePath, resp.body)
	}
}
