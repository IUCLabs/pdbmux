package main

import (
	"encoding/json"
	"time"
)

// record is a single PuppetDB result element kept as raw JSON so unknown fields
// survive the merge untouched. certname/report_timestamp are decoded only for
// merge decisions.
type record struct {
	Raw             json.RawMessage
	Certname        string
	ReportTimestamp string // only populated for /nodes records
}

// recordMeta is the subset we decode from any /nodes or /facts element to drive
// merge decisions.
type recordMeta struct {
	Certname        string `json:"certname"`
	ReportTimestamp string `json:"report_timestamp"`
}

// decodeRecords turns a raw PuppetDB JSON array into records, preserving each
// element verbatim in Raw. A body that is not a JSON array yields (nil, err).
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
		})
	}
	return out, nil
}

// parseTimestamp parses a PuppetDB RFC3339(nano) timestamp. Zero time on
// failure sorts oldest, so a backend with a well-formed newer timestamp wins.
func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	return time.Time{}
}

// mergeNodes dedupes /nodes records by certname, keeping the one with the newer
// report_timestamp. backends is the ordered list of (name, records) results;
// when timestamps tie (or both are zero), the earlier backend in the slice
// wins, so callers should order by precedence.
func mergeNodes(results []backendResult) []json.RawMessage {
	type pick struct {
		raw json.RawMessage
		ts  time.Time
	}
	best := map[string]pick{}
	var order []string
	for _, res := range results {
		for _, rec := range res.records {
			ts := parseTimestamp(rec.ReportTimestamp)
			cur, ok := best[rec.Certname]
			if !ok {
				best[rec.Certname] = pick{raw: rec.Raw, ts: ts}
				order = append(order, rec.Certname)
				continue
			}
			// Strictly-newer wins; ties keep the existing (earlier-backend) pick.
			if ts.After(cur.ts) {
				best[rec.Certname] = pick{raw: rec.Raw, ts: ts}
			}
		}
	}
	out := make([]json.RawMessage, 0, len(order))
	for _, cn := range order {
		out = append(out, best[cn].raw)
	}
	return out
}

// freshness maps certname -> backend name that holds that node's newest report.
type freshness map[string]string

// buildFreshness computes, per certname, which backend has the newer
// report_timestamp. results must be ordered by precedence; on a tie the
// earlier backend wins.
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

// mergeFacts merges /facts records at node granularity: for each certname, all
// facts from the winning backend are kept and the other backend's facts for
// that certname are dropped.
//
// The winner is chosen per certname by `owner(certname)`. Callers supply owner
// from either a freshness map (freshness merge) or a constant preferred backend
// (static merge). When owner returns a backend that has no facts for a certname
// (or a name not in results), records fall back to precedence order so a node
// present in only one backend still appears.
func mergeFacts(results []backendResult, owner func(certname string) string) []json.RawMessage {
	// Which backends actually returned facts for each certname, in precedence
	// order, so we can fall back if the chosen owner has none.
	present := map[string][]string{} // certname -> ordered backend names
	byKey := map[string][]json.RawMessage{}
	for _, res := range results {
		for _, rec := range res.records {
			key := rec.Certname + "\x00" + res.name
			if _, ok := byKey[key]; !ok {
				present[rec.Certname] = append(present[rec.Certname], res.name)
			}
			byKey[key] = append(byKey[key], rec.Raw)
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
		chosen := owner(cn)
		// Fall back to precedence order if the chosen backend has no facts here.
		if !contains(backends, chosen) {
			chosen = backends[0]
		}
		out = append(out, byKey[cn+"\x00"+chosen]...)
	}
	return out
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
