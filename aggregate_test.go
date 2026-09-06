package main

import (
	"encoding/json"
	"net/url"
	"reflect"
	"slices"
	"strings"
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

// mustAggregate parses a query that has to be a mergeable aggregate.
func mustAggregate(t *testing.T, q string) *aggregateSpec {
	t.Helper()
	spec, err := parseAggregate(q)
	if err != nil {
		t.Fatalf("parseAggregate(%s): %v", q, err)
	}
	if spec == nil {
		t.Fatalf("parseAggregate(%s) = nil, want an aggregate spec", q)
	}
	return spec
}

func aggNames(aggs []aggColumn) []string {
	out := make([]string, 0, len(aggs))
	for _, a := range aggs {
		out = append(out, a.name)
	}
	return out
}

func TestParseAggregate_ExtractWithGroupBy(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	if !slices.Equal(spec.keys, []string{"status"}) {
		t.Errorf("keys = %v, want [status]", spec.keys)
	}
	if !slices.Equal(aggNames(spec.aggs), []string{"count"}) {
		t.Errorf("aggs = %v, want [count]", aggNames(spec.aggs))
	}
	if spec.aggs[0].op != opAdd {
		t.Errorf("count op = %v, want opAdd", spec.aggs[0].op)
	}
}

func TestParseAggregate_GroupByAddsUnextractedField(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count","certname"]],["~","certname",".*"],["group_by","status"]]`)
	if !slices.Equal(spec.keys, []string{"status"}) {
		t.Errorf("keys = %v, want [status] from the group_by clause", spec.keys)
	}
	if !slices.Equal(aggNames(spec.aggs), []string{"count"}) {
		t.Errorf("aggs = %v, want [count]", aggNames(spec.aggs))
	}
}

func TestParseAggregate_OpPerFunction(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","min","line"],["function","max","line"],["function","sum","line"],["function","count"]]]`)
	want := []aggColumn{
		{name: "min", op: opMin},
		{name: "max", op: opMax},
		{name: "sum", op: opAdd},
		{name: "count", op: opAdd},
	}
	if !reflect.DeepEqual(spec.aggs, want) {
		t.Errorf("aggs = %v, want %v", spec.aggs, want)
	}
	if len(spec.keys) != 0 {
		t.Errorf("keys = %v, want none", spec.keys)
	}
}

// to_string and jsonb_typeof are row functions, so they identify a row rather
// than being folded into it.
func TestParseAggregate_RowFunctionsAreKeys(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","to_string","producer_timestamp","FMDD"],["function","jsonb_typeof","value"],["function","count"]]]`)
	if !slices.Equal(spec.keys, []string{"to_string", "jsonb_typeof"}) {
		t.Errorf("keys = %v, want [to_string jsonb_typeof]", spec.keys)
	}
	if !slices.Equal(aggNames(spec.aggs), []string{"count"}) {
		t.Errorf("aggs = %v, want [count]", aggNames(spec.aggs))
	}
}

func TestParseAggregate_GroupByOnAFunctionColumn(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","to_string","producer_timestamp","FMDD"],["function","count"]],["group_by",["function","to_string","producer_timestamp","FMDD"]]]`)
	if !slices.Equal(spec.keys, []string{"to_string"}) {
		t.Errorf("keys = %v, want [to_string] once", spec.keys)
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
		spec, err := parseAggregate(q)
		if err != nil {
			t.Errorf("parseAggregate(%q): unexpected error %v", q, err)
		}
		if spec != nil {
			t.Errorf("parseAggregate(%q) = %+v, want nil", q, spec)
		}
	}
}

// A function pdbmux has no combiner for is refused, never folded on a guess.
func TestParseAggregate_UnknownFunctionIsRefused(t *testing.T) {
	spec, err := parseAggregate(`["extract",[["function","stddev","line"]]]`)
	if err == nil {
		t.Fatalf("parseAggregate = %+v, want an error", spec)
	}
	if !strings.Contains(err.Error(), "stddev") {
		t.Errorf("error %q does not name the function", err)
	}
}

func TestCombineRows_SharedKeysAreAdded(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"count":3,"status":"changed"}`, `{"count":1,"status":"failed"}`)},
		{name: "b", records: rows(`{"count":4,"status":"changed"}`, `{"count":2,"status":"failed"}`)},
	}, spec.shape)

	want := []map[string]any{
		{"count": float64(7), "status": "changed"},
		{"count": float64(3), "status": "failed"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_SumIsAdded(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","sum","line"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"sum":30}`)},
		{name: "b", records: rows(`{"sum":12}`)},
	}, spec.shape)

	want := []map[string]any{{"sum": float64(42)}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// The bug this replaces: max folded by addition returned the sum of the
// backends' maxima, a number no backend ever held.
func TestCombineRows_MaxIsNotASum(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","max","line"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"max":20}`)},
		{name: "b", records: rows(`{"max":50}`)},
	}, spec.shape)

	got := decodeRows(t, merged)
	want := []map[string]any{{"max": float64(50)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged = %v, want %v", got, want)
	}
	if got[0]["max"] == float64(70) {
		t.Error("max was summed")
	}
}

func TestCombineRows_MinIsTheSmallest(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","min","line"]]]`)
	for _, tc := range []struct {
		name string
		a, b string
		want any
	}{
		{"later backend wins", `{"min":20}`, `{"min":5}`, float64(5)},
		{"earlier backend wins", `{"min":5}`, `{"min":20}`, float64(5)},
		{"negative values", `{"min":-1}`, `{"min":-9}`, float64(-9)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged := combineRows([]backendResult{
				{name: "a", records: rows(tc.a)},
				{name: "b", records: rows(tc.b)},
			}, spec.shape)
			got := decodeRows(t, merged)
			if len(got) != 1 || got[0]["min"] != tc.want {
				t.Errorf("merged = %v, want min %v", got, tc.want)
			}
		})
	}
}

// openvoxdb allows min/max on text columns, where the wrong answer used to be
// whichever backend answered first.
func TestCombineRows_MinMaxOnTextColumns(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","min","name"],["function","max","name"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"min":"kernel","max":"role"}`)},
		{name: "b", records: rows(`{"min":"extra_b","max":"uptime"}`)},
	}, spec.shape)

	want := []map[string]any{{"min": "extra_b", "max": "uptime"}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// A backend that matched nothing answers null, which is not an extreme.
func TestCombineRows_MinMaxIgnoreNulls(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","max","line"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"max":null}`)},
		{name: "b", records: rows(`{"max":7}`)},
	}, spec.shape)

	want := []map[string]any{{"max": float64(7)}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_MinMaxWithGroupBy(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","min","line"],["function","max","line"],"type"],["group_by","type"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"min":10,"max":20,"type":"File"}`, `{"min":1,"max":2,"type":"Stage"}`)},
		{name: "b", records: rows(`{"min":30,"max":50,"type":"File"}`)},
	}, spec.shape)

	want := []map[string]any{
		{"min": float64(10), "max": float64(50), "type": "File"},
		{"min": float64(1), "max": float64(2), "type": "Stage"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// avg is rewritten upstream into the sum and count of the same expression.
func TestParseAggregate_AvgIsRewrittenToSumAndCount(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","avg","line"]],["=","type","File"]]`)
	if !spec.avg {
		t.Fatal("spec does not record the avg rewrite")
	}
	const want = `["extract",[["function","sum","line"],["function","count","line"]],["=","type","File"]]`
	if spec.query != want {
		t.Errorf("upstream query = %s, want %s", spec.query, want)
	}
	if !slices.Equal(aggNames(spec.aggs), []string{"sum", "count"}) {
		t.Errorf("aggs = %v, want the sum and count helpers", aggNames(spec.aggs))
	}
}

func TestParseAggregate_AvgKeepsCompanionColumnsAndGroupBy(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","avg","line"],"type"],["group_by","type"]]`)
	const want = `["extract",[["function","sum","line"],"type",["function","count","line"]],["group_by","type"]]`
	if spec.query != want {
		t.Errorf("upstream query = %s, want %s", spec.query, want)
	}
	if !slices.Equal(spec.keys, []string{"type"}) {
		t.Errorf("keys = %v, want [type]", spec.keys)
	}
}

func TestCombineRows_AvgIsWeightedByCount(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","avg","line"]]]`)
	// Backend a averages 10 over 1 row, b averages 20 over 3: the true average
	// is 70/4, not the 15 an average of averages gives.
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"sum":10,"count":1}`)},
		{name: "b", records: rows(`{"sum":60,"count":3}`)},
	}, spec.shape)

	want := []map[string]any{{"avg": float64(17.5)}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// A single backend still gets the avg key back, not the helper columns.
func TestCombineRows_AvgFromOneBackend(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","avg","line"],"type"],["group_by","type"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"sum":30,"count":4,"type":"File"}`)},
		{name: "b", records: nil},
	}, spec.shape)

	want := []map[string]any{{"avg": float64(7.5), "type": "File"}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_AvgOverNoRowsIsNull(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","avg","line"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"sum":null,"count":0}`)},
		{name: "b", records: rows(`{"sum":null,"count":0}`)},
	}, spec.shape)

	want := []map[string]any{{"avg": nil}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_AvgWithGroupBy(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","avg","line"],"type"],["group_by","type"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"sum":10,"count":1,"type":"File"}`, `{"sum":8,"count":2,"type":"Stage"}`)},
		{name: "b", records: rows(`{"sum":60,"count":3,"type":"File"}`)},
	}, spec.shape)

	want := []map[string]any{
		{"avg": float64(17.5), "type": "File"},
		{"avg": float64(4), "type": "Stage"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// The helper columns take the response keys openvoxdb would give the client's
// own sum or count, so the pair cannot be served together.
func TestParseAggregate_AvgWithSumOrCountIsRefused(t *testing.T) {
	for _, q := range []string{
		`["extract",[["function","avg","line"],["function","sum","line"]]]`,
		`["extract",[["function","avg","line"],["function","count"]]]`,
	} {
		spec, err := parseAggregate(q)
		if err == nil {
			t.Errorf("parseAggregate(%s) = %+v, want a refusal", q, spec)
			continue
		}
		if !strings.Contains(err.Error(), "avg") {
			t.Errorf("error %q does not name avg", err)
		}
	}
}

func TestParseAggregate_AvgWithoutAColumnIsRefused(t *testing.T) {
	if spec, err := parseAggregate(`["extract",[["function","avg"]]]`); err == nil {
		t.Errorf("parseAggregate = %+v, want a refusal", spec)
	}
}

func TestParseAggregate_TwoAvgColumnsAreRefused(t *testing.T) {
	if spec, err := parseAggregate(`["extract",[["function","avg","line"],["function","avg","value"]]]`); err == nil {
		t.Errorf("parseAggregate = %+v, want a refusal", spec)
	}
}

// Every extract function names its column after itself, so a second one of the
// same name clashes whatever the function is — not only for avg.
func TestParseAggregate_RepeatedFunctionNameIsRefused(t *testing.T) {
	for _, fn := range []string{"count", "sum", "min", "max", "to_string", "jsonb_typeof", "avg"} {
		q := `["extract",[["function","` + fn + `","line"],["function","` + fn + `","type"]],["group_by","line","type"]]`
		spec, err := parseAggregate(q)
		if err == nil {
			t.Errorf("parseAggregate(%s) = %+v, want a refusal", q, spec)
			continue
		}
		if !strings.Contains(err.Error(), fn) {
			t.Errorf("error %q does not name the clashing column %q", err, fn)
		}
	}
}

// The same clash the other way round: a plain field takes the response key a
// later function column would name.
func TestParseAggregate_FieldClashingWithAFunctionIsRefused(t *testing.T) {
	const q = `["extract",["count",["function","count","certname"]],["group_by","count"]]`
	spec, err := parseAggregate(q)
	if err == nil {
		t.Fatalf("parseAggregate(%s) = %+v, want a refusal", q, spec)
	}
	if !strings.Contains(err.Error(), "count") {
		t.Errorf("error %q does not name the clashing column", err)
	}
}

// A repeated plain field projects the same value twice, so it is no clash: the
// duplicate carries nothing the grouping key has not already got.
func TestParseAggregate_RepeatedPlainFieldIsKept(t *testing.T) {
	spec := mustAggregate(t, `["extract",["type","type",["function","count","certname"]],["group_by","type"]]`)
	if !slices.Equal(spec.keys, []string{"type"}) {
		t.Errorf("keys = %v, want [type]", spec.keys)
	}
}

func TestCombineRows_DisjointKeysAreKept(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"count":3,"status":"changed"}`)},
		{name: "b", records: rows(`{"count":2,"status":"skipped"}`)},
	}, spec.shape)

	want := []map[string]any{
		{"count": float64(3), "status": "changed"},
		{"count": float64(2), "status": "skipped"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_SingleBackendRowKeepsUpstreamBytes(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	const raw = `{"count":3,"status":"changed","extra":{"kept":true}}`
	merged := combineRows([]backendResult{
		{name: "a", records: rows(raw)},
		{name: "b", records: nil},
	}, spec.shape)

	if len(merged) != 1 || string(merged[0]) != raw {
		t.Errorf("merged = %s, want the row verbatim %s", merged, raw)
	}
}

func TestCombineRows_NonNumericAggregateColumnIsNotZeroed(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"count":5,"status":"changed"}`)},
		{name: "b", records: rows(`{"count":null,"status":"changed"}`)},
	}, spec.shape)

	want := []map[string]any{{"count": float64(5), "status": "changed"}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want the numeric value preserved %v", got, want)
	}
}

func TestCombineRows_MissingAggregateColumnStartsFromTheNumericRow(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"status":"changed"}`)},
		{name: "b", records: rows(`{"count":6,"status":"changed"}`)},
	}, spec.shape)

	want := []map[string]any{{"count": float64(6), "status": "changed"}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_NonObjectRowsPassThrough(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","count"],"status"],["=","certname","h1"],["group_by","status"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`"surprise"`)},
		{name: "b", records: rows(`{"count":1,"status":"changed"}`)},
	}, spec.shape)

	if len(merged) != 2 || string(merged[0]) != `"surprise"` {
		t.Fatalf("merged = %s, want the non-object row kept as-is", merged)
	}
}

func TestCombineRows_NoKeyColumnsCollapseToOneRow(t *testing.T) {
	// ["extract",[["function","count"]],...] is a whole-estate count: one row
	// per backend, and the merged answer is their sum.
	spec := mustAggregate(t, `["extract",[["function","count"]],["=","certname","h1"]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"count":10}`)},
		{name: "b", records: rows(`{"count":32}`)},
	}, spec.shape)

	want := []map[string]any{{"count": float64(42)}}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// to_string compiles to a scalar expression, so openvoxdb answers one row per
// input row; with nothing to fold, every backend's rows are kept rather than
// collapsing into a single empty-key bucket.
func TestCombineRows_ToStringAloneDoesNotCollapse(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","to_string","producer_timestamp","FMDD"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"to_string":"01"}`, `{"to_string":"02"}`)},
		{name: "b", records: rows(`{"to_string":"02"}`, `{"to_string":"03"}`)},
	}, spec.shape)

	want := []map[string]any{
		{"to_string": "01"}, {"to_string": "02"},
		{"to_string": "02"}, {"to_string": "03"},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want every row kept %v", got, want)
	}
}

func TestCombineRows_ToStringGroupsItsCompanionCount(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","to_string","producer_timestamp","FMDD"],["function","count"]],["group_by",["function","to_string","producer_timestamp","FMDD"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"to_string":"01","count":2}`, `{"to_string":"02","count":3}`)},
		{name: "b", records: rows(`{"to_string":"02","count":4}`)},
	}, spec.shape)

	want := []map[string]any{
		{"to_string": "01", "count": float64(2)},
		{"to_string": "02", "count": float64(7)},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestCombineRows_JsonbTypeofGroups(t *testing.T) {
	spec := mustAggregate(t, `["extract",[["function","jsonb_typeof","value"],["function","count"]],["group_by",["function","jsonb_typeof","value"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"jsonb_typeof":"string","count":5}`)},
		{name: "b", records: rows(`{"jsonb_typeof":"string","count":6}`, `{"jsonb_typeof":"number","count":1}`)},
	}, spec.shape)

	want := []map[string]any{
		{"jsonb_typeof": "string", "count": float64(11)},
		{"jsonb_typeof": "number", "count": float64(1)},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

// A plain projected column keeps rows apart even when the query has no group_by.
func TestCombineRows_PlainColumnsAreGroupingKeys(t *testing.T) {
	spec := mustAggregate(t, `["extract",["type","title",["function","count"]]]`)
	merged := combineRows([]backendResult{
		{name: "a", records: rows(`{"type":"File","title":"/tmp/a","count":1}`)},
		{name: "b", records: rows(`{"type":"File","title":"/tmp/b","count":1}`, `{"type":"File","title":"/tmp/a","count":2}`)},
	}, spec.shape)

	want := []map[string]any{
		{"type": "File", "title": "/tmp/a", "count": float64(3)},
		{"type": "File", "title": "/tmp/b", "count": float64(1)},
	}
	if got := decodeRows(t, merged); !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestInferredShape_SplitsCountsFromIdentity(t *testing.T) {
	var row map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{"subject_type":"certname","subject":{"title":"h1"},"failures":1,"successes":2,"skips":null}`), &row); err != nil {
		t.Fatal(err)
	}
	sh := inferredShape(row)
	if !slices.Equal(sh.keys, []string{"subject", "subject_type"}) {
		t.Errorf("keys = %v, want [subject subject_type]", sh.keys)
	}
	// A null count is an empty aggregate, not part of the row's identity.
	if !slices.Equal(aggNames(sh.aggs), []string{"failures", "skips", "successes"}) {
		t.Errorf("aggs = %v, want [failures skips successes]", aggNames(sh.aggs))
	}
	for _, a := range sh.aggs {
		if a.op != opAdd {
			t.Errorf("%s op = %v, want opAdd", a.name, a.op)
		}
	}
}

func TestCombineRows_EventCountsPerSubject(t *testing.T) {
	merged := combineRows([]backendResult{
		{name: "a", records: rows(
			`{"subject_type":"certname","subject":{"title":"h1"},"failures":1,"successes":2,"noops":0,"skips":0}`,
			`{"subject_type":"certname","subject":{"title":"h2"},"failures":0,"successes":5,"noops":0,"skips":0}`,
		)},
		{name: "b", records: rows(
			`{"subject_type":"certname","subject":{"title":"h1"},"failures":3,"successes":4,"noops":1,"skips":0}`,
		)},
	}, inferredShape)

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

func TestDropOrderBy(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"removes the named field", `[{"field":"avg","order":"desc"}]`, ``},
		{"keeps the others", `[{"field":"avg"},{"field":"type"}]`, `[{"field":"type"}]`},
		{"leaves an unrelated order_by alone", `[{"field":"type"}]`, `[{"field":"type"}]`},
		{"drops an unparseable order_by", `not json`, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := url.Values{"order_by": {tc.in}}
			dropOrderBy(params, "avg")
			if got := params.Get("order_by"); got != tc.want {
				t.Errorf("order_by = %q, want %q", got, tc.want)
			}
		})
	}
}
