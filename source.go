package main

import (
	"encoding/json"
	"log"
	"regexp"
	"slices"
	"strings"
)

// sourceInjector owns the configured fact name for one request. A nil
// *sourceInjector is the feature-disabled case, so every method is nil-safe and
// callers need no branch.
type sourceInjector struct {
	name string
	// hosts maps backend name to the value emitted.
	hosts map[string]string
	// inject is false when the query shape rules synthesis out. Suppression of an
	// upstream fact of the same name does not depend on it.
	inject     bool
	suppressed int
}

// newSourceInjector returns nil only when the feature is off; a gated query
// yields an injector that suppresses but does not synthesise.
func (s *Server) newSourceInjector(query string, factEntity bool) *sourceInjector {
	if !s.cfg.SourceFactEnabled || s.cfg.SourceFact == "" {
		return nil
	}
	return &sourceInjector{name: s.cfg.SourceFact, hosts: s.hosts, inject: injectable(query, factEntity)}
}

// claims reports whether an upstream record carries the name pdbmux owns, and is
// keyed on the record's own name field: a projection that omits the name column
// yields records that cannot be identified, so they pass through. Suppression
// does not depend on the query gate.
func (si *sourceInjector) claims(factName string) bool {
	return si != nil && factName != "" && factName == si.name
}

// injects reports whether this response may carry the synthetic record.
func (si *sourceInjector) injects() bool {
	return si != nil && si.inject
}

// disableInject rules the synthetic record out for a request shape the query
// gate cannot see, leaving suppression on.
func (si *sourceInjector) disableInject() {
	if si != nil {
		si.inject = false
	}
}

// logSuppressed reports, once per request, that upstream records were dropped.
func (si *sourceInjector) logSuppressed(l *log.Logger) {
	if si == nil || si.suppressed == 0 || l == nil {
		return
	}
	l.Printf("info: dropped %d upstream %q fact record(s); pdbmux owns that fact name", si.suppressed, si.name)
}

// factRecord builds the synthetic /facts record, or nil when disabled.
// environment is copied from the node's real facts. All four keys of a fact
// record are always emitted, empty environment included: pypuppetdb indexes them
// directly (types.py Fact.create_from_dict), so an omitted key is a KeyError.
func (si *sourceInjector) factRecord(certname, backend, environment string) json.RawMessage {
	if !si.injects() {
		return nil
	}
	raw, err := json.Marshal(struct {
		Certname    string `json:"certname"`
		Environment string `json:"environment"`
		Name        string `json:"name"`
		Value       string `json:"value"`
	}{Certname: certname, Environment: environment, Name: si.name, Value: si.hosts[backend]})
	if err != nil {
		return nil
	}
	return raw
}

// stamp adds the provenance key to a /nodes record, overwriting any existing
// key of that name. A record that is not a JSON object passes through untouched.
func (si *sourceInjector) stamp(raw json.RawMessage, backend string) json.RawMessage {
	if !si.injects() {
		return raw
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil || obj == nil {
		return raw
	}
	value, err := json.Marshal(si.hosts[backend])
	if err != nil {
		return raw
	}
	obj[si.name] = value
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// injectable reports whether a response to this query may carry the synthetic
// record. Three shapes are excluded, each because the client asked for something
// the synthetic record is not part of:
//
//   - a query that is not an AST array, which includes every PQL-syntax query:
//     pdbmux cannot tell what it projects, so it changes nothing;
//   - an `extract` anywhere in the query's own projection scope, which projects a
//     column subset and, with a `["function", ...]` column, aggregates — injecting
//     there would corrupt the row shape or silently inflate a count(). Operands
//     scoped to a subquery are excluded; see hasExtract;
//   - on the facts entity, any outer constraint on `name`, which selects
//     specific facts. Subquery operands are not descended into: they choose which
//     nodes match, not which facts come back.
func injectable(query string, factEntity bool) bool {
	if query == "" {
		return true
	}
	var ast []json.RawMessage
	if json.Unmarshal([]byte(query), &ast) != nil || len(ast) == 0 {
		// Not an AST array pdbmux can reason about; leave the response alone.
		return false
	}
	var op string
	if json.Unmarshal(ast[0], &op) != nil {
		return false
	}
	if hasExtract(ast) {
		return false
	}
	if !factEntity {
		return true
	}
	return !constrainsField(ast, "name")
}

// hasExtract reports whether an extract appears anywhere in the query's own
// projection scope. openvoxdb accepts an extract as an operand of a boolean
// operator — engine.clj's user-node->plan-node sends every and/or/not operand
// back through itself (src/puppetlabs/puppetdb/query_eng/engine.clj:2697-2733)
// and valid-operator? lists "extract" (:2779-2784) — so the row shape can be
// rewritten below the top level, and the whole tree is walked to fail closed.
//
// Operators whose operand is scoped to a subquery are not descended into,
// because an extract there projects the subquery rather than the response:
//
//   - `in`, whose operand becomes InExpression's :subquery (:2705-2712);
//   - `subquery`, which the AST-rewrite stage expands into
//     ["in" cols ["extract" cols ["select_<entity>" expr]]] (:2111-2123)
//     before any plan node is built;
//   - `select_<entity>`, the explicit subquery form (:1889-1911), reachable
//     only under one of the two above in a query openvoxdb accepts.
func hasExtract(parts []json.RawMessage) bool {
	if len(parts) == 0 {
		return false
	}
	var op string
	if json.Unmarshal(parts[0], &op) != nil {
		return false
	}
	switch {
	case op == "extract":
		return true
	case op == "in", op == "subquery", strings.HasPrefix(op, "select_"):
		return false
	}
	for _, p := range parts[1:] {
		var sub []json.RawMessage
		if json.Unmarshal(p, &sub) != nil {
			continue
		}
		if hasExtract(sub) {
			return true
		}
	}
	return false
}

// constrainsField walks the boolean skeleton of an AST node looking for a
// comparison whose field operand is field. Only and/or/not are descended into;
// anything else, including the subquery operand of `in`, is left alone.
func constrainsField(parts []json.RawMessage, field string) bool {
	if len(parts) == 0 {
		return false
	}
	var op string
	if json.Unmarshal(parts[0], &op) != nil {
		return false
	}
	switch op {
	case "and", "or", "not":
		for _, p := range parts[1:] {
			var sub []json.RawMessage
			if json.Unmarshal(p, &sub) != nil {
				continue
			}
			if constrainsField(sub, field) {
				return true
			}
		}
		return false
	}
	if len(parts) < 2 {
		return false
	}
	var name string
	return json.Unmarshal(parts[1], &name) == nil && name == field
}

// sourceQuery returns the parsed /facts query when one of its name comparisons
// selects the owned fact and the whole query can be evaluated locally against a
// fact record; nil otherwise, which leaves the query on the gated path.
// ponytail: only a name match that is true for the owned fact selects it, so
// ["not",["=","name","osfamily"]] stays on the gated path and synthesises
// nothing; widen namesFact if a client needs negated selections.
func (si *sourceInjector) sourceQuery(query string) []json.RawMessage {
	if si == nil {
		return nil
	}
	var ast []json.RawMessage
	if json.Unmarshal([]byte(query), &ast) != nil {
		return nil
	}
	probe := factFields{Name: si.name}
	if _, ok := matchFact(ast, probe); !ok || !namesFact(ast, probe) {
		return nil
	}
	return ast
}

// factFields are the queryable columns of the facts entity (openvoxdb
// engine.clj facts-query). value is a string here: the synthetic value always is.
type factFields struct {
	Certname    string `json:"certname"`
	Environment string `json:"environment"`
	Name        string `json:"name"`
	Value       string `json:"value"`
}

func (f factFields) column(name string) (string, bool) {
	switch name {
	case "certname":
		return f.Certname, true
	case "environment":
		return f.Environment, true
	case "name":
		return f.Name, true
	case "value":
		return f.Value, true
	}
	return "", false
}

// namesFact reports whether a name comparison in the boolean skeleton matches f.
func namesFact(parts []json.RawMessage, f factFields) bool {
	var op, field string
	if len(parts) < 2 || json.Unmarshal(parts[0], &op) != nil {
		return false
	}
	switch op {
	case "and", "or", "not":
		for _, p := range parts[1:] {
			var sub []json.RawMessage
			if json.Unmarshal(p, &sub) == nil && namesFact(sub, f) {
				return true
			}
		}
		return false
	}
	if json.Unmarshal(parts[1], &field) != nil || field != "name" {
		return false
	}
	match, _ := matchFact(parts, f)
	return match
}

// matchFact evaluates a facts query against one record with openvoxdb's
// semantics for and/or/not, =, ~ and in-array. ok is false for anything else,
// including subqueries, so a caller can refuse rather than guess.
func matchFact(parts []json.RawMessage, f factFields) (match, ok bool) {
	var op string
	if len(parts) == 0 || json.Unmarshal(parts[0], &op) != nil {
		return false, false
	}
	switch op {
	case "and", "or":
		match = op == "and"
		for _, p := range parts[1:] {
			var sub []json.RawMessage
			if json.Unmarshal(p, &sub) != nil {
				return false, false
			}
			m, ok := matchFact(sub, f)
			if !ok {
				return false, false
			}
			if op == "and" {
				match = match && m
			} else {
				match = match || m
			}
		}
		return match, true
	case "not":
		var sub []json.RawMessage
		if len(parts) != 2 || json.Unmarshal(parts[1], &sub) != nil {
			return false, false
		}
		m, ok := matchFact(sub, f)
		return !m, ok
	}
	var field string
	if len(parts) != 3 || json.Unmarshal(parts[1], &field) != nil {
		return false, false
	}
	got, known := f.column(field)
	if !known {
		return false, false
	}
	switch op {
	case "=":
		var want any
		if json.Unmarshal(parts[2], &want) != nil {
			return false, false
		}
		return want == any(got), true
	case "~":
		var pattern string
		if json.Unmarshal(parts[2], &pattern) != nil {
			return false, false
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false, false
		}
		return re.MatchString(got), true
	case "in":
		var arr []json.RawMessage
		var tag string
		var values []any
		if json.Unmarshal(parts[2], &arr) != nil || len(arr) != 2 ||
			json.Unmarshal(arr[0], &tag) != nil || tag != "array" ||
			json.Unmarshal(arr[1], &values) != nil {
			return false, false
		}
		return slices.Contains(values, any(got)), true
	}
	return false, false
}

// certnameScope returns the top-level and's certname comparisons as a query,
// or "" when there are none. Pushing them upstream keeps every record of a
// selected node on every backend, so the merge's ownership is unchanged; an
// environment comparison is not pushed because it can hide the owner's records
// when backends disagree on a node's environment.
func certnameScope(ast []json.RawMessage) string {
	var op string
	if len(ast) < 2 || json.Unmarshal(ast[0], &op) != nil || op != "and" {
		return ""
	}
	var keep []string
	for _, p := range ast[1:] {
		var sub []json.RawMessage
		var field string
		if json.Unmarshal(p, &sub) != nil || len(sub) != 3 || json.Unmarshal(sub[1], &field) != nil || field != "certname" {
			continue
		}
		if _, ok := matchFact(sub, factFields{}); ok {
			keep = append(keep, string(p))
		}
	}
	switch len(keep) {
	case 0:
		return ""
	case 1:
		return keep[0]
	}
	return `["and",` + strings.Join(keep, ",") + `]`
}
