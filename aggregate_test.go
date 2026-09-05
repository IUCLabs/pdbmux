package main

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
)

func rows(raws ...string) []record {
	out := make([]record, 0, len(raws))
	for _, r := range raws {
		out = append(out, record{Raw: json.RawMessage(r)})
	}
	return out
}

// decodeRows turns a merged result set into comparable maps.
func decodeRows(t *testing.T, raws []json.RawMessage) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, len(raws))
	for _, raw := range raws {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		out = append(out, m)
	}
	return out
}

func TestParseAggregate_ExtractWithGroupBy(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	if spec == nil {
		t.Fatal("expected an aggregate spec")
	}
	if !slices.Equal(spec.keys, []string{"status"}) {
		t.Errorf("keys = %v, want [status]", spec.keys)
	}
	if !slices.Equal(spec.sums, []string{"count"}) {
		t.Errorf("sums = %v, want [count]", spec.sums)
	}
}

func TestParseAggregate_GroupByAddsUnextractedField(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count","certname"]],["~","certname",".*"],["group_by","status"]]`)
	if spec == nil {
		t.Fatal("expected an aggregate spec")
	}
	if !slices.Equal(spec.keys, []string{"status"}) {
		t.Errorf("keys = %v, want [status] from the group_by clause", spec.keys)
	}
	if !slices.Equal(spec.sums, []string{"count"}) {
		t.Errorf("sums = %v, want [count]", spec.sums)
	}
}

func TestParseAggregate_NoFunctionIsNotAggregate(t *testing.T) {
	for _, q := range []string{
		``,
		`["=","certname","h1"]`,
		`["extract",["certname","hash"],["=","certname","h1"]]`, // projection, still real reports
		`not json`,
		`["extract"]`,
		`{"not":"an array"}`,
	} {
		if spec := parseAggregate(q); spec != nil {
			t.Errorf("parseAggregate(%q) = %+v, want nil", q, spec)
		}
	}
}

func TestSumRows_SharedKeysAreAdded(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := sumRows([]backendResult{
		{name: "new", records: rows(`{"count":3,"status":"changed"}`, `{"count":1,"status":"failed"}`)},
		{name: "old", records: rows(`{"count":4,"status":"changed"}`, `{"count":2,"status":"failed"}`)},
	}, spec.columns)

	want := []map[string]any{
		{"count": float64(7), "status": "changed"},
		{"count": float64(3), "status": "failed"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestSumRows_DisjointKeysAreKept(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := sumRows([]backendResult{
		{name: "new", records: rows(`{"count":3,"status":"changed"}`)},
		{name: "old", records: rows(`{"count":2,"status":"skipped"}`)},
	}, spec.columns)

	want := []map[string]any{
		{"count": float64(3), "status": "changed"},
		{"count": float64(2), "status": "skipped"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestSumRows_SingleBackendRowKeepsUpstreamBytes(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	const raw = `{"count":3,"status":"changed","extra":{"kept":true}}`
	merged := sumRows([]backendResult{
		{name: "new", records: rows(raw)},
		{name: "old", records: nil},
	}, spec.columns)

	if len(merged) != 1 || string(merged[0]) != raw {
		t.Errorf("merged = %s, want the row verbatim %s", merged, raw)
	}
}

func TestSumRows_NonNumericAggregateColumnIsNotZeroed(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := sumRows([]backendResult{
		{name: "new", records: rows(`{"count":5,"status":"changed"}`)},
		{name: "old", records: rows(`{"count":null,"status":"changed"}`)},
	}, spec.columns)

	want := []map[string]any{{"count": float64(5), "status": "changed"}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want the numeric value preserved %v", got, want)
	}
}

func TestSumRows_MissingAggregateColumnStartsFromTheNumericRow(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := sumRows([]backendResult{
		{name: "new", records: rows(`{"status":"changed"}`)},
		{name: "old", records: rows(`{"count":6,"status":"changed"}`)},
	}, spec.columns)

	want := []map[string]any{{"count": float64(6), "status": "changed"}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestSumRows_NonObjectRowsPassThrough(t *testing.T) {
	spec := parseAggregate(`["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := sumRows([]backendResult{
		{name: "new", records: rows(`"surprise"`)},
		{name: "old", records: rows(`{"count":1,"status":"changed"}`)},
	}, spec.columns)

	if len(merged) != 2 || string(merged[0]) != `"surprise"` {
		t.Fatalf("merged = %s, want the non-object row kept as-is", merged)
	}
}

func TestSumRows_NoKeyColumnsCollapseToOneRow(t *testing.T) {
	// ["extract",[["function","count"]],...] is a whole-estate count: one row
	// per backend, and the merged answer is their sum.
	spec := parseAggregate(`["extract",[["function","count"]],["=","certname","h1"]]`)
	if spec == nil {
		t.Fatal("expected an aggregate spec")
	}
	merged := sumRows([]backendResult{
		{name: "new", records: rows(`{"count":10}`)},
		{name: "old", records: rows(`{"count":32}`)},
	}, spec.columns)

	want := []map[string]any{{"count": float64(42)}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestInferredColumns_SplitsCountsFromIdentity(t *testing.T) {
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{"subject_type":"certname","subject":{"title":"h1"},"failures":1,"successes":2,"skips":null}`), &row); err != nil {
		t.Fatal(err)
	}
	keys, sums := inferredColumns(row)
	if !slices.Equal(keys, []string{"subject", "subject_type"}) {
		t.Errorf("keys = %v, want [subject subject_type]", keys)
	}
	// A null count is an empty aggregate, not part of the row's identity.
	if !slices.Equal(sums, []string{"failures", "skips", "successes"}) {
		t.Errorf("sums = %v, want [failures skips successes]", sums)
	}
}

func TestSumRows_EventCountsPerSubject(t *testing.T) {
	merged := sumRows([]backendResult{
		{name: "new", records: rows(
			`{"subject_type":"certname","subject":{"title":"h1"},"failures":1,"successes":2,"noops":0,"skips":0}`,
			`{"subject_type":"certname","subject":{"title":"h2"},"failures":0,"successes":5,"noops":0,"skips":0}`,
		)},
		{name: "old", records: rows(
			`{"subject_type":"certname","subject":{"title":"h1"},"failures":3,"successes":4,"noops":1,"skips":0}`,
		)},
	}, inferredColumns)

	got := decodeRows(t, merged)
	want := []map[string]any{
		{"subject_type": "certname", "subject": map[string]any{"title": "h1"},
			"failures": float64(4), "successes": float64(6), "noops": float64(1), "skips": float64(0)},
		{"subject_type": "certname", "subject": map[string]any{"title": "h2"},
			"failures": float64(0), "successes": float64(5), "noops": float64(0), "skips": float64(0)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}
