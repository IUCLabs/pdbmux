package main

import (
	"encoding/json"
	"log"
	"strings"
)

// sourceInjector owns the configured fact name for one request. A nil
// *sourceInjector is the feature-disabled case, so every method is nil-safe and
// callers need no branch.
type sourceInjector struct {
	name string
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
	return &sourceInjector{name: s.cfg.SourceFact, inject: injectable(query, factEntity)}
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
	}{Certname: certname, Environment: environment, Name: si.name, Value: backend})
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
	value, err := json.Marshal(backend)
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
