package main

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// aggregateSpec names the columns of an extract/group_by result set: keys
// identify a row across backends, sums are the numeric aggregate columns added
// together.
type aggregateSpec struct {
	keys []string
	sums []string
}

// parseAggregate reads a PuppetDB AST query and returns the aggregate shape of
// its response, or nil when the query is not an aggregate — only a top-level
// `extract` carrying at least one `["function", ...]` column produces summable
// rows. Key columns are the plain (non-function) extract fields, unioned with an
// explicit `group_by` clause when the query has one.
func parseAggregate(query string) *aggregateSpec {
	if strings.TrimSpace(query) == "" {
		return nil
	}
	var ast []json.RawMessage
	if json.Unmarshal([]byte(query), &ast) != nil || len(ast) < 2 {
		return nil
	}
	var op string
	if json.Unmarshal(ast[0], &op) != nil || op != "extract" {
		return nil
	}
	var cols []json.RawMessage
	if json.Unmarshal(ast[1], &cols) != nil {
		return nil
	}

	spec := &aggregateSpec{}
	for _, col := range cols {
		var name string
		if json.Unmarshal(col, &name) == nil {
			spec.keys = appendUnique(spec.keys, name)
			continue
		}
		if fn, ok := functionName(col); ok {
			spec.sums = appendUnique(spec.sums, fn)
		}
	}
	if len(spec.sums) == 0 {
		return nil
	}
	for _, node := range ast[2:] {
		for _, f := range groupByFields(node) {
			spec.keys = appendUnique(spec.keys, f)
		}
	}
	return spec
}

// functionName returns the response column an extract function produces, which
// PuppetDB names after the function itself: ["function","count","certname"]
// yields a "count" column.
func functionName(col json.RawMessage) (string, bool) {
	var parts []json.RawMessage
	if json.Unmarshal(col, &parts) != nil || len(parts) < 2 {
		return "", false
	}
	var head, name string
	if json.Unmarshal(parts[0], &head) != nil || head != "function" {
		return "", false
	}
	if json.Unmarshal(parts[1], &name) != nil || name == "" {
		return "", false
	}
	return name, true
}

// groupByFields returns the field names of a ["group_by", ...] AST node, or nil
// for any other node.
func groupByFields(node json.RawMessage) []string {
	var parts []json.RawMessage
	if json.Unmarshal(node, &parts) != nil || len(parts) < 2 {
		return nil
	}
	var head string
	if json.Unmarshal(parts[0], &head) != nil || head != "group_by" {
		return nil
	}
	var out []string
	for _, p := range parts[1:] {
		var name string
		if json.Unmarshal(p, &name) == nil {
			out = append(out, name)
		}
	}
	return out
}

func appendUnique(s []string, v string) []string {
	if contains(s, v) {
		return s
	}
	return append(s, v)
}

// columns reports which fields of a row form its grouping key and which are
// summed. A spec is fixed by the query, so the row is ignored.
func (a *aggregateSpec) columns(map[string]json.RawMessage) ([]string, []string) {
	return a.keys, a.sums
}

// inferredColumns derives an event-counts row's shape from the row itself: the
// counts to add (successes, failures, noops, skips, total) are its numeric
// fields, plus any null one — PuppetDB nulls an aggregate column when a backend
// matched nothing — and everything else, subject_type/subject/summarize_by,
// identifies the row. Those endpoints have a fixed response shape with no
// numeric key field, so nothing summable is mistaken for identity.
func inferredColumns(row map[string]json.RawMessage) ([]string, []string) {
	var keys, sums []string
	for name, val := range row {
		if isJSONNumber(val) || isJSONNull(val) {
			sums = append(sums, name)
			continue
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)
	sort.Strings(sums)
	return keys, sums
}

// isJSONNumber reports whether a raw JSON value is a number.
func isJSONNumber(raw json.RawMessage) bool {
	v := strings.TrimSpace(string(raw))
	if v == "" {
		return false
	}
	return v[0] == '-' || (v[0] >= '0' && v[0] <= '9')
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

// sumGroup accumulates the rows sharing one grouping key.
type sumGroup struct {
	raw    json.RawMessage            // first contributing row, verbatim
	row    map[string]json.RawMessage // its decoded fields
	totals map[string]float64         // running sum per aggregate column
	merged bool                       // a second row was folded in
}

// sumRows folds each backend's aggregate rows into one row per grouping key,
// adding the numeric aggregate columns. columns decides, per row, which fields
// are the key and which are summed.
//
// A row that is not a JSON object passes through untouched, as does the sole row
// of a key only one backend reported — those keep their upstream bytes. An
// aggregate column that is absent or non-numeric in a later row is left at the
// first backend's value rather than being coerced to zero. results must be
// ordered by precedence; output keeps first-seen order.
func sumRows(results []backendResult, columns func(map[string]json.RawMessage) ([]string, []string)) []json.RawMessage {
	type slot struct {
		raw   json.RawMessage // passthrough row, when group is nil
		group *sumGroup
	}
	var order []slot
	groups := map[string]*sumGroup{}

	for _, res := range results {
		for _, rec := range res.records {
			var row map[string]json.RawMessage
			if json.Unmarshal(rec.Raw, &row) != nil {
				order = append(order, slot{raw: rec.Raw})
				continue
			}
			keys, sums := columns(row)
			k := groupKey(row, keys)
			g, ok := groups[k]
			if !ok {
				g = &sumGroup{raw: rec.Raw, row: row, totals: map[string]float64{}}
				for _, s := range sums {
					if n, ok := numberOf(row[s]); ok {
						g.totals[s] = n
					}
				}
				groups[k] = g
				order = append(order, slot{group: g})
				continue
			}
			for _, s := range sums {
				n, ok := numberOf(row[s])
				if !ok {
					continue
				}
				if _, seen := g.totals[s]; !seen {
					// First numeric value for a column the earlier row lacked.
					g.totals[s] = 0
				}
				g.totals[s] += n
				g.merged = true
			}
		}
	}

	out := make([]json.RawMessage, 0, len(order))
	for _, sl := range order {
		if sl.group == nil {
			out = append(out, sl.raw)
			continue
		}
		out = append(out, sl.group.encode())
	}
	return out
}

// encode renders a group back to JSON, reusing the first row's bytes when
// nothing was added to it.
func (g *sumGroup) encode() json.RawMessage {
	if !g.merged {
		return g.raw
	}
	row := make(map[string]json.RawMessage, len(g.row))
	for k, v := range g.row {
		row[k] = v
	}
	for col, total := range g.totals {
		row[col] = json.RawMessage(strconv.FormatFloat(total, 'f', -1, 64))
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return g.raw
	}
	return raw
}

// groupKey builds a row's identity from the named fields' verbatim JSON values.
// Both backends run the same PuppetDB serialiser, so byte equality is a sound
// comparison for object-valued keys such as event-counts' subject. An absent
// field is distinct from any present value.
func groupKey(row map[string]json.RawMessage, keys []string) string {
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		if v, ok := row[k]; ok {
			b.Write(v)
		} else {
			b.WriteByte(1)
		}
		b.WriteByte(0)
	}
	return b.String()
}

// numberOf decodes a raw JSON number, reporting ok=false for anything else so
// non-numeric aggregate columns are carried through instead of summed.
func numberOf(raw json.RawMessage) (float64, bool) {
	if !isJSONNumber(raw) {
		return 0, false
	}
	var n float64
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	return n, true
}
