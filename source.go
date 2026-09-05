package main

import "encoding/json"

// sourceInjector synthesises the provenance fact naming the backend whose data
// won the merge for a given certname. A nil *sourceInjector is the disabled
// case, so every method is nil-safe and callers need no branch.
type sourceInjector struct {
	name string
}

// newSourceInjector returns nil when injection is off for this request.
func (s *Server) newSourceInjector(query string, factEntity bool) *sourceInjector {
	if !s.cfg.SourceFactEnabled || s.cfg.SourceFact == "" {
		return nil
	}
	if !injectable(query, factEntity) {
		return nil
	}
	return &sourceInjector{name: s.cfg.SourceFact}
}

// claims reports whether an upstream record is the one the injector replaces.
func (si *sourceInjector) claims(factName string) bool {
	return si != nil && factName != "" && factName == si.name
}

// factRecord builds the synthetic /facts record, or nil when disabled.
// environment is copied from the node's real facts. All four keys of a fact
// record are always emitted, empty environment included: pypuppetdb indexes them
// directly (types.py Fact.create_from_dict), so an omitted key is a KeyError.
func (si *sourceInjector) factRecord(certname, backend, environment string) json.RawMessage {
	if si == nil {
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
	if si == nil {
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
// record. Two shapes are excluded, both because the client asked for something
// the synthetic record is not part of:
//
//   - a top-level `extract`, which projects a column subset and, with a
//     `["function", ...]` column, aggregates — injecting there would corrupt the
//     row shape or silently inflate a count();
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
	if op == "extract" {
		return false
	}
	if !factEntity {
		return true
	}
	return !constrainsField(ast, "name")
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
