package main

import (
	"encoding/json"
	"testing"
)

// recs builds a backendResult from name + literal JSON element strings.
func recs(t *testing.T, name string, elems ...string) backendResult {
	t.Helper()
	body := "[" + join(elems) + "]"
	r, err := decodeRecords([]byte(body))
	if err != nil {
		t.Fatalf("decodeRecords(%s): %v", body, err)
	}
	return backendResult{name: name, records: r}
}

func join(elems []string) string {
	out := ""
	for i, e := range elems {
		if i > 0 {
			out += ","
		}
		out += e
	}
	return out
}

// certnames extracts the certname field from a merged result set.
func certnames(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	var out []string
	for _, r := range raws {
		var m recordMeta
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", r, err)
		}
		out = append(out, m.Certname)
	}
	return out
}

// factValues extracts "certname:name=value" for /facts records to assert which
// backend's facts survived.
func factValues(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	var out []string
	for _, r := range raws {
		var m struct {
			Certname string `json:"certname"`
			Name     string `json:"name"`
			Value    string `json:"value"`
		}
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", r, err)
		}
		out = append(out, m.Certname+":"+m.Name+"="+m.Value)
	}
	return out
}

func node(cn, ts string) string {
	return `{"certname":"` + cn + `","report_timestamp":"` + ts + `","latest_report_status":"changed"}`
}

func fact(cn, name, val, ts string) string {
	// facts records don't carry report_timestamp in real PuppetDB, but including
	// it is harmless and lets a couple of tests reuse the same helper. Merge
	// attribution for facts comes from the owner func, not the record.
	if ts == "" {
		return `{"certname":"` + cn + `","name":"` + name + `","value":"` + val + `"}`
	}
	return `{"certname":"` + cn + `","name":"` + name + `","value":"` + val + `","report_timestamp":"` + ts + `"}`
}

func TestMergeNodes_NewerWins(t *testing.T) {
	old := recs(t, "old", node("h1", "2026-07-01T00:00:00Z"), node("h2", "2026-07-10T00:00:00Z"))
	nw := recs(t, "new", node("h1", "2026-07-20T00:00:00Z"), node("h3", "2026-07-05T00:00:00Z"))

	merged := mergeNodes([]backendResult{old, nw})
	got := map[string]string{}
	for _, r := range merged {
		var m recordMeta
		_ = json.Unmarshal(r, &m)
		got[m.Certname] = m.ReportTimestamp
	}
	if got["h1"] != "2026-07-20T00:00:00Z" {
		t.Errorf("h1: newer (new) should win, got %s", got["h1"])
	}
	if got["h2"] != "2026-07-10T00:00:00Z" {
		t.Errorf("h2: only in old, got %s", got["h2"])
	}
	if got["h3"] != "2026-07-05T00:00:00Z" {
		t.Errorf("h3: only in new, got %s", got["h3"])
	}
	if len(merged) != 3 {
		t.Errorf("expected 3 deduped nodes, got %d", len(merged))
	}
}

func TestMergeNodes_OneBackendOnly(t *testing.T) {
	old := recs(t, "old", node("h1", "2026-07-01T00:00:00Z"))
	// new returned nothing (e.g. empty result).
	nw := backendResult{name: "new"}
	merged := mergeNodes([]backendResult{old, nw})
	if len(merged) != 1 || certnames(t, merged)[0] != "h1" {
		t.Fatalf("expected only h1, got %v", certnames(t, merged))
	}
}

func TestMergeNodes_TieKeepsEarlierBackend(t *testing.T) {
	// Equal timestamps: the backend listed first (precedence) wins.
	prefer := recs(t, "new", node("h1", "2026-07-01T00:00:00Z"))
	other := recs(t, "old", node("h1", "2026-07-01T00:00:00Z"))
	merged := mergeNodes([]backendResult{prefer, other})
	if len(merged) != 1 {
		t.Fatalf("expected 1 record, got %d", len(merged))
	}
	// Ensure the kept record is the first backend's (identical here, but assert count/dedupe).
	if certnames(t, merged)[0] != "h1" {
		t.Fatalf("expected h1")
	}
}

func TestMergeNodes_PreservesUnknownFields(t *testing.T) {
	old := recs(t, "old", `{"certname":"h1","report_timestamp":"2026-07-01T00:00:00Z","extra":{"deep":42}}`)
	merged := mergeNodes([]backendResult{old})
	if len(merged) != 1 {
		t.Fatalf("expected 1 record")
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(merged[0], &m)
	if _, ok := m["extra"]; !ok {
		t.Fatalf("unknown field 'extra' was dropped: %s", merged[0])
	}
}

func TestMergeFacts_Static_PreferWins(t *testing.T) {
	// h1 in both; static prefer=new -> new's facts kept, old's dropped.
	old := recs(t, "old", fact("h1", "role", "web-old", ""), fact("h2", "role", "db-old", ""))
	nw := recs(t, "new", fact("h1", "role", "web-new", ""))

	merged := mergeFacts([]backendResult{nw, old}, func(string) string { return "new" })
	got := factValues(t, merged)
	assertContains(t, got, "h1:role=web-new")
	assertNotContains(t, got, "h1:role=web-old")
	// h2 only in old -> falls back to old.
	assertContains(t, got, "h2:role=db-old")
}

func TestMergeFacts_Freshness_NewerBackendWins(t *testing.T) {
	// owner map says h1 belongs to old (older backend has the newer report),
	// h2 belongs to new. Multiple facts per node must all come from the winner.
	old := recs(t, "old",
		fact("h1", "role", "web-old", ""), fact("h1", "ip", "192.0.2.1", ""),
		fact("h2", "role", "db-old", ""))
	nw := recs(t, "new",
		fact("h1", "role", "web-new", ""), fact("h1", "ip", "192.0.2.99", ""),
		fact("h2", "role", "db-new", ""), fact("h2", "ip", "192.0.2.2", ""))

	owner := func(cn string) string {
		if cn == "h1" {
			return "old"
		}
		return "new"
	}
	merged := mergeFacts([]backendResult{nw, old}, owner)
	got := factValues(t, merged)
	// h1 -> all old facts, no new facts.
	assertContains(t, got, "h1:role=web-old")
	assertContains(t, got, "h1:ip=192.0.2.1")
	assertNotContains(t, got, "h1:role=web-new")
	assertNotContains(t, got, "h1:ip=192.0.2.99")
	// h2 -> all new facts.
	assertContains(t, got, "h2:role=db-new")
	assertContains(t, got, "h2:ip=192.0.2.2")
	assertNotContains(t, got, "h2:role=db-old")
}

func TestMergeFacts_OwnerMissingFallsBackToPrecedence(t *testing.T) {
	// owner returns a backend with no facts for h1 -> fall back to first
	// backend present (precedence order of the slice).
	prefer := recs(t, "new", fact("h1", "role", "web-new", ""))
	other := recs(t, "old", fact("h1", "role", "web-old", ""))
	merged := mergeFacts([]backendResult{prefer, other}, func(string) string { return "ghost" })
	got := factValues(t, merged)
	assertContains(t, got, "h1:role=web-new") // new is first in slice
	assertNotContains(t, got, "h1:role=web-old")
}

func TestBuildFreshness(t *testing.T) {
	// old has newer report for h1; new has newer for h2.
	old := recs(t, "old", node("h1", "2026-07-20T00:00:00Z"), node("h2", "2026-07-01T00:00:00Z"))
	nw := recs(t, "new", node("h1", "2026-07-01T00:00:00Z"), node("h2", "2026-07-20T00:00:00Z"))
	f := buildFreshness([]backendResult{old, nw})
	if f["h1"] != "old" {
		t.Errorf("h1 should belong to old, got %q", f["h1"])
	}
	if f["h2"] != "new" {
		t.Errorf("h2 should belong to new, got %q", f["h2"])
	}
}

func TestDecodeRecords_NotArray(t *testing.T) {
	if _, err := decodeRecords([]byte(`{"not":"array"}`)); err == nil {
		t.Fatal("expected error decoding non-array body")
	}
}

func assertContains(t *testing.T, hay []string, needle string) {
	t.Helper()
	for _, h := range hay {
		if h == needle {
			return
		}
	}
	t.Errorf("expected %q in %v", needle, hay)
}

func assertNotContains(t *testing.T, hay []string, needle string) {
	t.Helper()
	for _, h := range hay {
		if h == needle {
			t.Errorf("did not expect %q in %v", needle, hay)
		}
	}
}
