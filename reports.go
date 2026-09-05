package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// results must be ordered by precedence; a key func returning ok=false means the record has no identity and is always kept.
func mergeUnion(results []backendResult, key func(record) (string, bool)) []json.RawMessage {
	seen := make(map[string]bool)
	out := []json.RawMessage{}
	for _, res := range results {
		for _, rec := range res.records {
			if k, ok := key(rec); ok {
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			out = append(out, rec.Raw)
		}
	}
	return out
}

// extract/group_by rows are synthetic and carry no hash, so two backends can legitimately emit identical ones.
func reportKey(rec record) (string, bool) {
	if rec.Hash == "" {
		return "", false
	}
	return "hash\x00" + rec.Hash, true
}

// Events carry no id, but byte-identical events from the same PuppetDB serialiser are the same change.
func rawKey(rec record) (string, bool) { return "raw\x00" + string(rec.Raw), true }

type orderField struct {
	Field string
	Desc  bool
}

// order_by is a JSON array of {"field": ..., "order": "asc"|"desc"} objects.
func parseOrderBy(s string) ([]orderField, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var raw []struct {
		Field string `json:"field"`
		Order string `json:"order"`
	}
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, fmt.Errorf("order_by is not a JSON array: %w", err)
	}
	out := make([]orderField, 0, len(raw))
	for _, r := range raw {
		if r.Field == "" {
			return nil, fmt.Errorf("order_by entry is missing a field")
		}
		out = append(out, orderField{Field: r.Field, Desc: strings.EqualFold(r.Order, "desc")})
	}
	return out, nil
}

// Each backend ordered only its own slice, so the union is re-sorted here; stable, so ties keep backend precedence.
func sortRecords(recs []json.RawMessage, order []orderField) {
	if len(order) == 0 || len(recs) < 2 {
		return
	}
	objs := make([]map[string]any, len(recs))
	for i, raw := range recs {
		_ = json.Unmarshal(raw, &objs[i]) // non-objects sort as all-missing fields
	}
	idx := make([]int, len(recs))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		oa, ob := objs[idx[a]], objs[idx[b]]
		for _, f := range order {
			c := compareValues(oa[f.Field], ob[f.Field])
			if c == 0 {
				continue
			}
			if f.Desc {
				return c > 0
			}
			return c < 0
		}
		return false
	})
	sorted := make([]json.RawMessage, len(recs))
	for i, j := range idx {
		sorted[i] = recs[j]
	}
	copy(recs, sorted)
}

// Unlike types order by kind (null < bool < number < string), so a missing field sorts first.
func compareValues(a, b any) int {
	ra, rb := valueRank(a), valueRank(b)
	if ra != rb {
		if ra < rb {
			return -1
		}
		return 1
	}
	switch av := a.(type) {
	case bool:
		bv := b.(bool)
		switch {
		case av == bv:
			return 0
		case bv:
			return -1
		default:
			return 1
		}
	case float64:
		bv := b.(float64)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case string:
		return strings.Compare(av, b.(string))
	}
	return 0
}

func valueRank(v any) int {
	switch v.(type) {
	case nil:
		return 0
	case bool:
		return 1
	case float64:
		return 2
	case string:
		return 3
	default:
		return 4
	}
}

type paging struct {
	limit     int // -1 when unset
	offset    int
	order     []orderField
	wantTotal bool
}

func parsePaging(v url.Values) (paging, error) {
	p := paging{limit: -1}
	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return p, fmt.Errorf("limit must be a non-negative integer, got %q", s)
		}
		p.limit = n
	}
	if s := v.Get("offset"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return p, fmt.Errorf("offset must be a non-negative integer, got %q", s)
		}
		p.offset = n
	}
	order, err := parseOrderBy(v.Get("order_by"))
	if err != nil {
		return p, err
	}
	p.order = order
	p.wantTotal = v.Get("include_total") == "true"
	return p, nil
}

// Backends are asked for the first offset+limit records with no offset; the offset is applied to the union instead.
func (p paging) upstreamParams(in url.Values) url.Values {
	out := url.Values{}
	for k, vs := range in {
		out[k] = append([]string(nil), vs...)
	}
	out.Del("offset")
	if p.limit >= 0 {
		out.Set("limit", strconv.Itoa(p.limit+p.offset))
	}
	return out
}

func (p paging) apply(recs []json.RawMessage) []json.RawMessage {
	if p.offset >= len(recs) {
		return []json.RawMessage{}
	}
	recs = recs[p.offset:]
	if p.limit >= 0 && p.limit < len(recs) {
		recs = recs[:p.limit]
	}
	return recs
}

// Returns -1 when no backend reported a count; duplicates count once per backend, so the sum is an upper bound.
func sumTotals(results []backendResult) int {
	total := -1
	for _, res := range results {
		if res.total < 0 {
			continue
		}
		if total < 0 {
			total = 0
		}
		total += res.total
	}
	return total
}
