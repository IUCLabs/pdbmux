package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// combineOp is how one aggregate column's per-backend values become one value.
type combineOp int

const (
	opAdd combineOp = iota
	opMin
	opMax
)

// aggColumn is one aggregate column of a result row: the JSON key openvoxdb
// names it and the operation that folds the backends' values together.
type aggColumn struct {
	name string
	op   combineOp
}

// rowShape splits a result row into the columns that identify it and the
// aggregate columns that are folded. finish, when set, rewrites the merged row
// before it is encoded and forces re-encoding even for a single-backend row.
type rowShape struct {
	keys   []string
	aggs   []aggColumn
	finish func(map[string]json.RawMessage)
}

// openvoxdb's accepted extract functions are the keys of pdb-fns->pg-fns,
// src/puppetlabs/puppetdb/query_eng/engine.clj:234-241, and each response column
// is named after the function itself (compile-fnexpression, engine.clj:1488-1495).
// count and sum add across backends; min and max take the extreme, on text
// columns as well as numeric ones, which openvoxdb allows because the numeric
// guard applies only to comparison clauses (engine.clj:2483-2490). avg is not
// combinable from the shard rows at all, so it is rewritten upstream — see
// rewriteAvg.
var aggregateOps = map[string]combineOp{
	"count": opAdd,
	"sum":   opAdd,
	"min":   opMin,
	"max":   opMax,
}

// to_string and jsonb_typeof compile to scalar Postgres expressions, so they
// yield one row per input row rather than an aggregate: they name an ordinary
// projected column and identify a row instead of being folded into one.
var rowFns = map[string]bool{
	"to_string":    true,
	"jsonb_typeof": true,
}

const avgColumn = "avg"

// aggregateSpec is the merge shape of an extract query's response.
type aggregateSpec struct {
	keys  []string
	aggs  []aggColumn
	query string // rewritten upstream query, empty when the request's own is used
	avg   bool   // the client asked for avg; sum and count are pdbmux's helpers
}

// parseAggregate reads a PuppetDB AST query and returns the merge shape of its
// response, or nil when the query is not an extract carrying a `["function",...]`
// column. A non-nil error means the query is an aggregate pdbmux cannot merge
// and must be refused rather than answered with a wrong number.
//
// Key columns are the plain extract fields, the row-function columns and any
// `group_by` clause; only genuine aggregates are folded.
func parseAggregate(query string) (*aggregateSpec, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}
	var ast []json.RawMessage
	if json.Unmarshal([]byte(query), &ast) != nil || len(ast) < 2 {
		return nil, nil
	}
	var op string
	if json.Unmarshal(ast[0], &op) != nil || op != "extract" {
		return nil, nil
	}
	var cols []json.RawMessage
	if json.Unmarshal(ast[1], &cols) != nil {
		return nil, nil
	}

	spec := &aggregateSpec{}
	var avgArgs []json.RawMessage
	sawFunction := false
	projected := map[string]bool{}
	fnColumns := map[string]bool{}
	for _, col := range cols {
		var name string
		if json.Unmarshal(col, &name) == nil {
			// A field repeating another field is harmless — the copy holds the same
			// value as the key it duplicates — but one taking a function's name is
			// the same clash the other way round.
			if fnColumns[name] {
				return nil, columnClash(name)
			}
			projected[name] = true
			spec.keys = appendUnique(spec.keys, name)
			continue
		}
		fn, args, ok := functionColumn(col)
		if !ok {
			continue
		}
		sawFunction = true
		if projected[fn] {
			return nil, columnClash(fn)
		}
		projected[fn], fnColumns[fn] = true, true
		switch {
		case rowFns[fn]:
			spec.keys = appendUnique(spec.keys, fn)
		case fn == avgColumn:
			if len(args) == 0 {
				return nil, fmt.Errorf("extract function avg needs a column to average")
			}
			spec.avg, avgArgs = true, args
		default:
			combine, known := aggregateOps[fn]
			if !known {
				return nil, fmt.Errorf("extract function %q cannot be merged across backends", fn)
			}
			spec.aggs = append(spec.aggs, aggColumn{name: fn, op: combine})
		}
	}
	if !sawFunction {
		return nil, nil
	}
	for _, node := range ast[2:] {
		for _, f := range groupByFields(node) {
			if hasAgg(spec.aggs, f) || (spec.avg && f == avgColumn) {
				return nil, groupByClash(f)
			}
			spec.keys = appendUnique(spec.keys, f)
		}
	}
	if spec.avg {
		if err := spec.rewriteAvg(ast, cols, avgArgs); err != nil {
			return nil, err
		}
	}
	return spec, nil
}

// columnClash refuses a projection naming one response column twice. openvoxdb
// aliases every extract column after its function, so a repeat comes back as an
// order-dependent "<name>_2" that is neither a grouping key nor a folded
// aggregate, leaving the first backend's value frozen in the merged row.
func columnClash(name string) error {
	return fmt.Errorf("extract projects the column %q more than once: openvoxdb returns the repeat as %q, which pdbmux can neither key on nor fold", name, name+"_2")
}

// groupByClash refuses a group_by naming an aggregate column: the same key
// cannot both identify a row and be folded across backends. openvoxdb rejects
// the shape too, so refusing here is a 400 instead of a failed upstream query.
func groupByClash(name string) error {
	return fmt.Errorf("group_by names the aggregate column %q: pdbmux folds that column across backends, so it cannot also be a grouping key", name)
}

// rewriteAvg replaces the client's avg column with the sum and count of the same
// expression, so the true weighted average can be computed from the shards:
// Postgres avg(x) is sum(x)/count(x), and both of those do combine.
func (a *aggregateSpec) rewriteAvg(ast, cols []json.RawMessage, args []json.RawMessage) error {
	for _, taken := range []string{"sum", "count"} {
		// openvoxdb names a column after its function, so a second one of the same
		// name comes back as an order-dependent "<name>_2" rather than its own key.
		if contains(a.keys, taken) || hasAgg(a.aggs, taken) {
			return fmt.Errorf("avg cannot be merged alongside a %q column: pdbmux rewrites avg into an upstream sum and count, which openvoxdb would return under the same response key", taken)
		}
	}
	sumCol, err := functionNode("sum", args)
	if err != nil {
		return err
	}
	countCol, err := functionNode("count", args)
	if err != nil {
		return err
	}

	out := make([]json.RawMessage, 0, len(cols)+1)
	for _, col := range cols {
		if fn, _, ok := functionColumn(col); ok && fn == avgColumn {
			out = append(out, sumCol)
			continue
		}
		out = append(out, col)
	}
	out = append(out, countCol)

	rewritten := append([]json.RawMessage(nil), ast...)
	encoded, err := json.Marshal(out)
	if err != nil {
		return err
	}
	rewritten[1] = encoded
	query, err := json.Marshal(rewritten)
	if err != nil {
		return err
	}
	a.query = string(query)
	a.aggs = append(a.aggs, aggColumn{name: "sum", op: opAdd}, aggColumn{name: "count", op: opAdd})
	return nil
}

func functionNode(fn string, args []json.RawMessage) (json.RawMessage, error) {
	parts := make([]json.RawMessage, 0, len(args)+2)
	parts = append(parts, json.RawMessage(`"function"`), json.RawMessage(`"`+fn+`"`))
	parts = append(parts, args...)
	return json.Marshal(parts)
}

// functionColumn returns the response column an extract function produces, which
// openvoxdb names after the function itself — ["function","count","certname"]
// yields a "count" column — along with the function's arguments.
func functionColumn(col json.RawMessage) (string, []json.RawMessage, bool) {
	var parts []json.RawMessage
	if json.Unmarshal(col, &parts) != nil || len(parts) < 2 {
		return "", nil, false
	}
	var head, name string
	if json.Unmarshal(parts[0], &head) != nil || head != "function" {
		return "", nil, false
	}
	if json.Unmarshal(parts[1], &name) != nil || name == "" {
		return "", nil, false
	}
	return name, parts[2:], true
}

// groupByFields returns the response columns a ["group_by", ...] AST node names,
// or nil for any other node. An entry may be a plain field or a function node,
// which groups on that function's own column.
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
			continue
		}
		if fn, _, ok := functionColumn(p); ok {
			out = append(out, fn)
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

func hasAgg(s []aggColumn, name string) bool {
	for _, x := range s {
		if x.name == name {
			return true
		}
	}
	return false
}

// shape reports a row's merge shape. A spec is fixed by the query, so the row is
// ignored.
func (a *aggregateSpec) shape(map[string]json.RawMessage) rowShape {
	sh := rowShape{keys: a.keys, aggs: a.aggs}
	if a.avg {
		sh.finish = finishAvg
	}
	return sh
}

// finishAvg turns the summed helper columns into the avg key the client asked
// for. An empty count is openvoxdb's own answer for an average over no rows.
func finishAvg(row map[string]json.RawMessage) {
	sum, sumOK := numberOf(row["sum"])
	count, countOK := numberOf(row["count"])
	delete(row, "sum")
	delete(row, "count")
	if !sumOK || !countOK || count == 0 {
		row[avgColumn] = json.RawMessage("null")
		return
	}
	row[avgColumn] = json.RawMessage(strconv.FormatFloat(sum/count, 'f', -1, 64))
}

// inferredShape derives an event-counts row's shape from the row itself: the
// counts to add (successes, failures, noops, skips, total) are its numeric
// fields, plus any null one — openvoxdb nulls an aggregate column when a backend
// matched nothing — and everything else, subject_type/subject/summarize_by,
// identifies the row. Those endpoints have a fixed response shape with no
// numeric key field, so nothing summable is mistaken for identity.
func inferredShape(row map[string]json.RawMessage) rowShape {
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
	aggs := make([]aggColumn, 0, len(sums))
	for _, s := range sums {
		aggs = append(aggs, aggColumn{name: s, op: opAdd})
	}
	return rowShape{keys: keys, aggs: aggs}
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

// mergeGroup accumulates the rows sharing one grouping key.
type mergeGroup struct {
	raw      json.RawMessage            // first contributing row, verbatim
	row      map[string]json.RawMessage // its decoded fields
	totals   map[string]float64         // running sum per additive column
	extremes map[string]json.RawMessage // running min/max per extreme column
	finish   func(map[string]json.RawMessage)
	merged   bool // a later row changed the group
}

// combineRows folds each backend's aggregate rows into one row per grouping key,
// combining each aggregate column by its own operation. shape decides, per row,
// which fields are the key and how each aggregate column combines.
//
// A row that is not a JSON object passes through untouched, as does the sole
// row of a key only one backend reported and every row of a shape with no
// aggregate column to fold — those keep their upstream bytes. An aggregate
// column that is absent or null in a later row leaves the group's value alone
// rather than being coerced to zero. results come in configured backend order
// and the output keeps first-seen order, a tie-break only.
func combineRows(results []backendResult, shape func(map[string]json.RawMessage) rowShape) []json.RawMessage {
	type slot struct {
		raw   json.RawMessage // passthrough row, when group is nil
		group *mergeGroup
	}
	var order []slot
	groups := map[string]*mergeGroup{}

	for _, res := range results {
		for _, rec := range res.records {
			var row map[string]json.RawMessage
			if json.Unmarshal(rec.Raw, &row) != nil {
				order = append(order, slot{raw: rec.Raw})
				continue
			}
			sh := shape(row)
			if len(sh.aggs) == 0 {
				// Only row functions were projected, so there is nothing to fold
				// and each backend's rows stand on their own.
				order = append(order, slot{raw: rec.Raw})
				continue
			}
			k := groupKey(row, sh.keys)
			g, ok := groups[k]
			if !ok {
				g = newMergeGroup(rec.Raw, row, sh)
				groups[k] = g
				order = append(order, slot{group: g})
				continue
			}
			g.fold(row, sh.aggs)
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

func newMergeGroup(raw json.RawMessage, row map[string]json.RawMessage, sh rowShape) *mergeGroup {
	g := &mergeGroup{
		raw:      raw,
		row:      row,
		totals:   map[string]float64{},
		extremes: map[string]json.RawMessage{},
		finish:   sh.finish,
	}
	for _, c := range sh.aggs {
		if c.op == opAdd {
			if n, ok := numberOf(row[c.name]); ok {
				g.totals[c.name] = n
			}
			continue
		}
		if v, ok := row[c.name]; ok && !isJSONNull(v) {
			g.extremes[c.name] = v
		}
	}
	return g
}

func (g *mergeGroup) fold(row map[string]json.RawMessage, aggs []aggColumn) {
	for _, c := range aggs {
		if c.op == opAdd {
			n, ok := numberOf(row[c.name])
			if !ok {
				continue
			}
			if _, seen := g.totals[c.name]; !seen {
				// First numeric value for a column the earlier row lacked.
				g.totals[c.name] = 0
			}
			g.totals[c.name] += n
			g.merged = true
			continue
		}
		v, ok := row[c.name]
		if !ok || isJSONNull(v) {
			continue
		}
		cur, seen := g.extremes[c.name]
		if seen && !extremeWins(v, cur, c.op) {
			continue
		}
		g.extremes[c.name] = v
		g.merged = true
	}
}

// extremeWins reports whether candidate replaces the running min or max. Values
// are compared decoded, so min/max works on the text columns openvoxdb allows
// them on as well as on numbers.
func extremeWins(candidate, current json.RawMessage, op combineOp) bool {
	var a, b any
	if json.Unmarshal(candidate, &a) != nil || json.Unmarshal(current, &b) != nil {
		return false
	}
	c := compareValues(a, b)
	if op == opMin {
		return c < 0
	}
	return c > 0
}

// encode renders a group back to JSON, reusing the first row's bytes when
// nothing changed it and no finish step has to rewrite it.
func (g *mergeGroup) encode() json.RawMessage {
	if !g.merged && g.finish == nil {
		return g.raw
	}
	row := make(map[string]json.RawMessage, len(g.row))
	for k, v := range g.row {
		row[k] = v
	}
	for col, total := range g.totals {
		row[col] = json.RawMessage(strconv.FormatFloat(total, 'f', -1, 64))
	}
	for col, v := range g.extremes {
		row[col] = v
	}
	if g.finish != nil {
		g.finish(row)
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return g.raw
	}
	return raw
}

// groupKey builds a row's identity from the named fields' verbatim JSON values.
// Every backend runs the same PuppetDB serialiser, so byte equality is a sound
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
