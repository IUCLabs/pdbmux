package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// mergeUnion concatenates every backend's records and drops duplicates by key.
// Reports and events are immutable history, so a certname that migrated between
// PuppetDBs legitimately has records in both and the union — not a per-node
// winner — is the correct merged view. results must be ordered by precedence;
// the first backend holding a key supplies the record. A key func returning
// ok=false means the record has no dedupe identity and is always kept.
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

// reportKey identifies a report by its content hash, which PuppetDB guarantees
// is unique per report. An `extract`/`group_by` query returns synthetic rows
// with no hash and no identity — two backends can emit byte-identical aggregate
// rows that both count — so those are never deduped.
func reportKey(rec record) (string, bool) {
	if rec.Hash == "" {
		return "", false
	}
	return "hash\x00" + rec.Hash, true
}

// rawKey identifies a record by its verbatim JSON. Events carry no unique id,
// but two byte-identical events from the same PuppetDB serialiser describe the
// same resource change, so raw equality is a safe dedupe key.
func rawKey(rec record) (string, bool) { return "raw\x00" + string(rec.Raw), true }

// orderField is one entry of PuppetDB's order_by param.
type orderField struct {
	Field string
	Desc  bool
}

// parseOrderBy decodes PuppetDB's order_by param, a JSON array of
// {"field":..., "order":"asc"|"desc"} objects. An empty param yields no fields.
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

// sortRecords re-sorts a merged record set by order. Each backend only ordered
// its own slice, so the union has to be ordered again here. The sort is stable,
// so ties keep backend precedence order.
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

// compareValues orders two decoded JSON values. Unlike types are ordered by
// kind (null < bool < number < string) so a missing field always sorts first.
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

// paging holds the PuppetDB paging params a merged endpoint has to re-apply
// itself: each backend applies limit/offset to its own result set only, so the
// proxy must page the union instead.
type paging struct {
	limit     int // -1 when unset
	offset    int
	order     []orderField
	wantTotal bool
}

// parsePaging reads limit, offset, order_by and include_total from a request's
// query params.
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

// upstreamParams rewrites the client's params for the fan-out. A backend must
// return everything that could land in the merged page, so it is asked for the
// first offset+limit records and the offset is applied locally instead.
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

// apply slices the merged, ordered record set down to the requested page.
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

// sumTotals adds up the X-Records counts the backends reported, ignoring any
// backend that did not send one. It returns -1 when no backend reported a count.
// Deduped records are counted once per backend, so the total is an upper bound.
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
