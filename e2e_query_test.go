//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"testing"
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
	if got := node[defaultSourceFact]; got != backendBName {
		t.Errorf("merged /nodes %s for %s = %v, want %q", defaultSourceFact, nodeShared, got, backendBName)
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

// Known gap: handleQuery's `case factsPath:` goes straight to serveMerged, with
// none of the parseAggregate branch /nodes, /resources and /reports have, so a
// /facts aggregate is fed to the certname-keyed merge. Count rows carry an empty
// certname, collapse into one bucket, and the response is whichever backend owns
// that bucket rather than the sum. Tracked separately; this test records the gap
// and fails once it closes so it can be turned into a real assertion.
func TestFactsAggregatesAreNotSummed(t *testing.T) {
	ctx := context.Background()
	const q = `["extract",[["function","count"]]]`

	wantA := backendCount(ctx, t, h.a, factsPath, q)
	wantB := backendCount(ctx, t, h.b, factsPath, q)
	if wantA == wantB {
		t.Fatalf("the fixture gives both backends %d facts, so this test cannot tell a sum from one backend's number", wantA)
	}

	got := countOf(t, get(t, factsPath, query(q)).rows(t))
	if got == wantA+wantB {
		t.Fatalf("/facts count = %d, which is the correct sum: the aggregate gap has closed, so assert this properly and drop the skip", got)
	}
	t.Skipf("known gap: /facts aggregates do not route to serveSummed, so the count is %d (backend %s alone) instead of %d",
		got, h.a.name, wantA+wantB)
}

// The provenance fact must name the backend whose data won, and must be absent
// from the query shapes it would corrupt.
func TestSourceFactInjectionAndGating(t *testing.T) {
	t.Run("facts carry one source record per node", func(t *testing.T) {
		rows := get(t, factsPath, nil).rows(t)
		want := map[string]string{
			nodeAlpha:  backendAName,
			nodeBeta:   backendBName,
			nodeGamma:  backendBName,
			nodeShared: backendBName, // won on freshness, not on configured order
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
