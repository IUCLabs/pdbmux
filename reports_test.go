package main

import (
	"encoding/json"
	"net/url"
	"slices"
	"testing"
)

func TestMergeUnion_KeepsBothBackendsHistory(t *testing.T) {
	old := recs(t, "old", report("h1", "r1", "2026-07-01T00:00:00Z"))
	nw := recs(t, "new", report("h1", "r2", "2026-07-02T00:00:00Z"))
	merged := mergeUnion([]backendResult{nw, old}, reportKey)
	if got := hashesOf(t, merged); !slices.Equal(got, []string{"r2", "r1"}) {
		t.Errorf("union = %v, want both reports in precedence order", got)
	}
}

func TestMergeUnion_DedupesSharedHash(t *testing.T) {
	dup := report("h1", "r1", "2026-07-01T00:00:00Z")
	merged := mergeUnion([]backendResult{recs(t, "new", dup), recs(t, "old", dup)}, reportKey)
	if got := hashesOf(t, merged); !slices.Equal(got, []string{"r1"}) {
		t.Errorf("union = %v, want a single r1", got)
	}
}

func TestMergeUnion_HashlessRowsAreAllKept(t *testing.T) {
	// extract/group_by queries return synthetic rows with no hash; dropping the
	// second backend's rows as "duplicates" would silently lose half the data.
	old := recs(t, "old", `{"status":"changed","count":3}`)
	nw := recs(t, "new", `{"status":"changed","count":5}`)
	merged := mergeUnion([]backendResult{old, nw}, reportKey)
	if len(merged) != 2 {
		t.Errorf("expected both aggregate rows, got %d: %v", len(merged), merged)
	}
}

func TestMergeUnion_EventsDedupeOnRawIdentity(t *testing.T) {
	same := event("h1", "r1", "Package[nginx]")
	other := event("h1", "r1", "Service[nginx]")
	merged := mergeUnion([]backendResult{recs(t, "new", same, other), recs(t, "old", same)}, rawKey)
	if len(merged) != 2 {
		t.Errorf("expected 2 distinct events, got %d: %v", len(merged), merged)
	}
}

func TestParseOrderBy(t *testing.T) {
	got, err := parseOrderBy(`[{"field":"receive_time","order":"desc"},{"field":"certname"}]`)
	if err != nil {
		t.Fatal(err)
	}
	want := []orderField{{Field: "receive_time", Desc: true}, {Field: "certname"}}
	if !slices.Equal(got, want) {
		t.Errorf("parseOrderBy = %v, want %v", got, want)
	}

	if got, err := parseOrderBy("  "); err != nil || got != nil {
		t.Errorf("empty order_by = %v, %v; want nil, nil", got, err)
	}
	for _, bad := range []string{`receive_time`, `[{"order":"desc"}]`} {
		if _, err := parseOrderBy(bad); err == nil {
			t.Errorf("parseOrderBy(%q) should have failed", bad)
		}
	}
}

func TestSortRecords_MultipleFieldsAndStability(t *testing.T) {
	raws := rawsOf(t,
		`{"certname":"b","status":"failed","hash":"r1"}`,
		`{"certname":"a","status":"changed","hash":"r2"}`,
		`{"certname":"a","status":"changed","hash":"r3"}`,
		`{"certname":"a","status":"failed","hash":"r4"}`,
	)
	sortRecords(raws, []orderField{{Field: "certname"}, {Field: "status", Desc: true}})
	// certname asc, then status desc; r2/r3 tie fully and keep input order.
	if got := hashesOf(t, raws); !slices.Equal(got, []string{"r4", "r2", "r3", "r1"}) {
		t.Errorf("sorted = %v, want [r4 r2 r3 r1]", got)
	}
}

func TestSortRecords_MissingFieldSortsFirst(t *testing.T) {
	raws := rawsOf(t,
		`{"hash":"r1","receive_time":"2026-07-01T00:00:00Z"}`,
		`{"hash":"r2"}`,
	)
	sortRecords(raws, []orderField{{Field: "receive_time"}})
	if got := hashesOf(t, raws); !slices.Equal(got, []string{"r2", "r1"}) {
		t.Errorf("sorted = %v, want the record missing the field first", got)
	}
}

func TestSortRecords_NoOrderLeavesInputOrder(t *testing.T) {
	raws := rawsOf(t, `{"hash":"r1"}`, `{"hash":"r2"}`)
	sortRecords(raws, nil)
	if got := hashesOf(t, raws); !slices.Equal(got, []string{"r1", "r2"}) {
		t.Errorf("sorted = %v, want unchanged", got)
	}
}

func TestCompareValues_AcrossKinds(t *testing.T) {
	cases := []struct {
		a, b any
		want int
	}{
		{nil, false, -1},
		{false, true, -1},
		{true, 1.0, -1},
		{1.0, 2.0, -1},
		{2.0, 2.0, 0},
		{2.0, "x", -1},
		{"a", "b", -1},
		{"b", "a", 1},
	}
	for _, c := range cases {
		if got := compareValues(c.a, c.b); got != c.want {
			t.Errorf("compareValues(%v, %v) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := compareValues(c.b, c.a); got != -c.want {
			t.Errorf("compareValues(%v, %v) = %d, want %d (antisymmetry)", c.b, c.a, got, -c.want)
		}
	}
}

func TestParsePaging(t *testing.T) {
	p, err := parsePaging(url.Values{
		"limit":         {"25"},
		"offset":        {"50"},
		"include_total": {"true"},
		"order_by":      {`[{"field":"receive_time","order":"desc"}]`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.limit != 25 || p.offset != 50 || !p.wantTotal || len(p.order) != 1 {
		t.Fatalf("parsePaging = %+v", p)
	}

	if p, err := parsePaging(nil); err != nil || p.limit != -1 || p.offset != 0 || p.wantTotal {
		t.Errorf("empty params = %+v, %v; want limit=-1 and no paging", p, err)
	}
	for _, bad := range []url.Values{{"limit": {"-1"}}, {"limit": {"x"}}, {"offset": {"x"}}} {
		if _, err := parsePaging(bad); err == nil {
			t.Errorf("parsePaging(%v) should have failed", bad)
		}
	}
}

func TestPagingUpstreamParams(t *testing.T) {
	in := url.Values{
		"query":  {`["=","certname","h1"]`},
		"limit":  {"25"},
		"offset": {"50"},
	}
	p, err := parsePaging(in)
	if err != nil {
		t.Fatal(err)
	}
	out := p.upstreamParams(in)
	if out.Get("limit") != "75" {
		t.Errorf("upstream limit = %q, want 75 (offset+limit)", out.Get("limit"))
	}
	if out.Has("offset") {
		t.Errorf("upstream offset = %q, want it dropped", out.Get("offset"))
	}
	if out.Get("query") != in.Get("query") {
		t.Errorf("query should pass through verbatim, got %q", out.Get("query"))
	}
	if in.Get("limit") != "25" {
		t.Errorf("upstreamParams must not mutate the caller's params, limit is now %q", in.Get("limit"))
	}
}

func TestPagingUpstreamParams_NoLimitLeavesQueryUnbounded(t *testing.T) {
	in := url.Values{"offset": {"5"}}
	p, err := parsePaging(in)
	if err != nil {
		t.Fatal(err)
	}
	out := p.upstreamParams(in)
	if out.Has("limit") || out.Has("offset") {
		t.Errorf("upstream params = %v, want neither limit nor offset", out)
	}
}

func TestPagingApply(t *testing.T) {
	raws := rawsOf(t, `{"hash":"r1"}`, `{"hash":"r2"}`, `{"hash":"r3"}`)
	cases := []struct {
		name string
		page paging
		want []string
	}{
		{name: "window", page: paging{limit: 1, offset: 1}, want: []string{"r2"}},
		{name: "limit past end", page: paging{limit: 10}, want: []string{"r1", "r2", "r3"}},
		{name: "offset past end", page: paging{limit: 2, offset: 9}, want: nil},
		{name: "unset limit", page: paging{limit: -1, offset: 2}, want: []string{"r3"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := hashesOf(t, c.page.apply(raws))
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !slices.Equal(got, c.want) {
				t.Errorf("apply = %v, want %v", got, c.want)
			}
		})
	}
}

func TestSumTotals(t *testing.T) {
	if got := sumTotals([]backendResult{{total: 40}, {total: 60}}); got != 100 {
		t.Errorf("sumTotals = %d, want 100", got)
	}
	if got := sumTotals([]backendResult{{total: -1}, {total: 7}}); got != 7 {
		t.Errorf("sumTotals should skip backends without a count, got %d", got)
	}
	if got := sumTotals([]backendResult{{total: -1}, {total: -1}}); got != -1 {
		t.Errorf("sumTotals with no counts = %d, want -1", got)
	}
}

// rawsOf builds a raw record slice from literal JSON elements.
func rawsOf(t *testing.T, elems ...string) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, 0, len(elems))
	for _, e := range elems {
		out = append(out, json.RawMessage(e))
	}
	return out
}

// hashesOf extracts the hash field from raw records, in order.
func hashesOf(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(raws))
	for _, r := range raws {
		var m recordMeta
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", r, err)
		}
		out = append(out, m.Hash)
	}
	return out
}
