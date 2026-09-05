package main

import (
	"encoding/json"
	"time"
)

// Raw is kept verbatim so unknown PuppetDB fields survive the merge.
type record struct {
	Raw             json.RawMessage
	Certname        string
	ReportTimestamp string // only populated for /nodes records
	Hash            string // only populated for /reports records
	Name            string // only populated for /facts records
	Environment     string
}

type recordMeta struct {
	Certname        string `json:"certname"`
	ReportTimestamp string `json:"report_timestamp"`
	Hash            string `json:"hash"`
	Name            string `json:"name"`
	Environment     string `json:"environment"`
}

func decodeRecords(body []byte) ([]record, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, err
	}
	out := make([]record, 0, len(raws))
	for _, raw := range raws {
		var m recordMeta
		_ = json.Unmarshal(raw, &m) // best-effort; missing fields stay zero
		out = append(out, record{
			Raw:             raw,
			Certname:        m.Certname,
			ReportTimestamp: m.ReportTimestamp,
			Hash:            m.Hash,
			Name:            m.Name,
			Environment:     m.Environment,
		})
	}
	return out, nil
}

// An unparseable timestamp yields the zero time, which sorts oldest.
func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	return time.Time{}
}

// Ties keep the earlier backend's record — a deterministic tie-break, not a preference.
// A non-nil inject stamps each surviving record with the backend that supplied it.
func mergeNodes(results []backendResult, inject *sourceInjector) []json.RawMessage {
	type pick struct {
		raw     json.RawMessage
		ts      time.Time
		backend string
	}
	best := map[string]pick{}
	var order []string
	for _, res := range results {
		for _, rec := range res.records {
			ts := parseTimestamp(rec.ReportTimestamp)
			cur, ok := best[rec.Certname]
			if !ok {
				best[rec.Certname] = pick{raw: rec.Raw, ts: ts, backend: res.name}
				order = append(order, rec.Certname)
				continue
			}
			if ts.After(cur.ts) {
				best[rec.Certname] = pick{raw: rec.Raw, ts: ts, backend: res.name}
			}
		}
	}
	out := make([]json.RawMessage, 0, len(order))
	for _, cn := range order {
		p := best[cn]
		out = append(out, inject.stamp(p.raw, p.backend))
	}
	return out
}

// certname -> name of the backend holding that node's newest report.
type freshness map[string]string

// Ties keep the earlier backend — a deterministic tie-break, not a preference.
func buildFreshness(results []backendResult) freshness {
	type pick struct {
		backend string
		ts      time.Time
	}
	best := map[string]pick{}
	for _, res := range results {
		for _, rec := range res.records {
			ts := parseTimestamp(rec.ReportTimestamp)
			cur, ok := best[rec.Certname]
			if !ok || ts.After(cur.ts) {
				best[rec.Certname] = pick{backend: res.name, ts: ts}
			}
		}
	}
	f := make(freshness, len(best))
	for cn, p := range best {
		f[cn] = p.backend
	}
	return f
}

// owner names the winning backend per certname; a nil owner (static merge), or one holding no facts for that certname, falls back to configured order.
// A non-nil inject appends the synthetic source fact after each certname's block, naming the backend that won.
func mergeFacts(results []backendResult, owner func(certname string) string, inject *sourceInjector) []json.RawMessage {
	present := map[string][]string{} // certname -> backend names, in configured order
	byKey := map[string][]record{}
	for _, res := range results {
		for _, rec := range res.records {
			key := rec.Certname + "\x00" + res.name
			if _, ok := byKey[key]; !ok {
				present[rec.Certname] = append(present[rec.Certname], res.name)
			}
			byKey[key] = append(byKey[key], rec)
		}
	}

	// Emit in first-seen certname order for stable output.
	var order []string
	seen := map[string]bool{}
	for _, res := range results {
		for _, rec := range res.records {
			if !seen[rec.Certname] {
				seen[rec.Certname] = true
				order = append(order, rec.Certname)
			}
		}
	}

	out := []json.RawMessage{}
	for _, cn := range order {
		backends := present[cn]
		chosen := ""
		if owner != nil {
			chosen = owner(cn)
		}
		if !contains(backends, chosen) {
			chosen = backends[0]
		}
		recs := byKey[cn+"\x00"+chosen]
		for _, rec := range recs {
			// An upstream fact of the same name is dropped: pdbmux's own value is authoritative.
			if inject.claims(rec.Name) {
				continue
			}
			out = append(out, rec.Raw)
		}
		if synth := inject.factRecord(cn, chosen, environmentOf(recs)); synth != nil {
			out = append(out, synth)
		}
	}
	return out
}

func environmentOf(recs []record) string {
	for _, rec := range recs {
		if rec.Environment != "" {
			return rec.Environment
		}
	}
	return ""
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
