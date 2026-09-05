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
}

type recordMeta struct {
	Certname        string `json:"certname"`
	ReportTimestamp string `json:"report_timestamp"`
	Hash            string `json:"hash"`
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

// results must be ordered by precedence: ties keep the earlier backend's record.
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

// certname -> name of the backend holding that node's newest report.
type freshness map[string]string

// results must be ordered by precedence: ties keep the earlier backend.
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

// owner names the winning backend per certname; when it holds no facts for that certname, precedence order wins.
func mergeFacts(results []backendResult, owner func(certname string) string) []json.RawMessage {
	present := map[string][]string{} // certname -> backend names, in precedence order
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
