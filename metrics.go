package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// metricsPrefix covers PuppetDB's Jolokia surface, which sits at the server root
// rather than under /pdb: pypuppetdb's metric() reads /metrics/v2/read/<mbean>,
// lists via /metrics/v2/list, and falls back to /metrics/v1/mbeans/<mbean>.
const metricsPrefix = "/metrics/"

type mergeRule int

const (
	ruleSum mergeRule = iota
	ruleMin
	ruleMax
	ruleMean
)

// mergeRuleFor picks how one numeric MBean attribute combines across backends.
// The default is a sum: the metrics Puppetboard renders are population counts
// (num-nodes, num-resources, queue depth, command totals) whose estate-wide
// value is the total. The exceptions are attributes describing a distribution or
// a bound rather than a quantity, where adding two servers' numbers yields a
// figure that was never true of either.
func mergeRuleFor(attr string) mergeRule {
	a := strings.ToLower(attr)
	switch a {
	case "min":
		return ruleMin
	case "max", "uptime", "starttime":
		return ruleMax
	case "mean", "median", "stddev":
		return ruleMean
	}
	if strings.HasSuffix(a, "percentile") {
		return ruleMean
	}
	return ruleSum
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	// MBean names carry Jolokia's !-escapes under percent-encoding; the raw path
	// is forwarded so neither layer is lost.
	path := r.URL.EscapedPath()
	alive, ok := s.aliveRaw(w, s.fanOutRaw(r.Context(), path, r.URL.RawQuery), path)
	if !ok {
		return
	}

	body, ok := mergeMetrics(alive, metricAttribute(r.URL.Path))
	if !ok {
		// Every backend answered 2xx but none carried a mergeable payload — a
		// Jolokia error envelope, or a body that is not a JSON object. Replaying
		// the first keeps the upstream error text the client expects.
		writeRaw(w, alive[0])
		return
	}
	// The merged body is the backends' own payload reshaped, so it keeps their
	// content type rather than announcing a different one.
	contentType := alive[0].contentType
	if contentType == "" {
		contentType = jsonContentType
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(body)
}

// metricAttribute names the single attribute a read asked for, when the URL
// carries one (/metrics/v2/read/<mbean>/<attribute>), so a scalar response body
// still gets the right numeric rule. Empty means the response is an object whose
// own keys name its attributes.
func metricAttribute(path string) string {
	rest, ok := strings.CutPrefix(path, metricsPrefix)
	if !ok {
		return ""
	}
	parts := strings.Split(rest, "/") // v2/read/<mbean>[/<attribute>]
	if len(parts) < 4 {
		return ""
	}
	return parts[len(parts)-1]
}

// mergeMetrics folds the backends' Jolokia responses into one. A response is
// either a Jolokia envelope ({"request":…,"value":…,"status":200}), where only
// "value" merges and the rest comes from the first backend, or a bare attribute
// object (metrics/v1), which merges whole. ok=false means nothing was mergeable.
func mergeMetrics(alive []rawResult, attr string) ([]byte, bool) {
	var objs []map[string]json.RawMessage
	for _, res := range alive {
		var obj map[string]json.RawMessage
		if decodeJSON(res.body, &obj) != nil || obj == nil {
			continue
		}
		// Jolokia reports a per-request failure inside an HTTP 200.
		if n, ok := numberOf(obj["status"]); ok && (n < 200 || n >= 300) {
			continue
		}
		objs = append(objs, obj)
	}
	if len(objs) == 0 {
		return nil, false
	}

	_, hasValue := objs[0]["value"]
	_, hasStatus := objs[0]["status"]
	if !hasValue || !hasStatus {
		vals := make([]any, 0, len(objs))
		for _, obj := range objs {
			vals = append(vals, decodedObject(obj))
		}
		return marshal(mergeMetricValue(vals, attr))
	}

	vals := make([]any, 0, len(objs))
	for _, obj := range objs {
		var v any
		if decodeJSON(obj["value"], &v) == nil {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return nil, false
	}
	merged, ok := marshal(mergeMetricValue(vals, attr))
	if !ok {
		return nil, false
	}

	out := make(map[string]json.RawMessage, len(objs[0]))
	for k, v := range objs[0] {
		out[k] = v
	}
	out["value"] = merged
	// The envelope timestamp says when the answer was produced; the newest one
	// describes the merged answer.
	if ts, ok := maxField(objs, "timestamp"); ok {
		out["timestamp"] = ts
	}
	return marshal(out)
}

// maxField returns the largest numeric value of a field across the responses.
func maxField(objs []map[string]json.RawMessage, field string) (json.RawMessage, bool) {
	var best json.RawMessage
	var bestN float64
	for _, obj := range objs {
		n, ok := numberOf(obj[field])
		if !ok {
			continue
		}
		if best == nil || n > bestN {
			best, bestN = obj[field], n
		}
	}
	return best, best != nil
}

// mergeMetricValue folds one attribute's value from every backend into one.
// Objects merge key by key over the union of keys, so a backend missing an
// attribute still contributes the rest. Numbers combine by the attribute's rule.
// Anything else — strings, booleans, arrays, nulls, or a mix of kinds — keeps
// the first backend's value, there being no sound way to add them.
func mergeMetricValue(vals []any, attr string) any {
	if len(vals) == 0 {
		return nil
	}
	if len(vals) == 1 {
		return vals[0]
	}

	objs := make([]map[string]any, 0, len(vals))
	for _, v := range vals {
		if m, ok := v.(map[string]any); ok {
			objs = append(objs, m)
		}
	}
	if len(objs) == len(vals) {
		out := map[string]any{}
		for _, m := range objs {
			for k := range m {
				if _, done := out[k]; done {
					continue
				}
				sub := make([]any, 0, len(objs))
				for _, o := range objs {
					if v, ok := o[k]; ok {
						sub = append(sub, v)
					}
				}
				out[k] = mergeMetricValue(sub, k)
			}
		}
		return out
	}

	nums := make([]float64, 0, len(vals))
	for _, v := range vals {
		n, ok := v.(json.Number)
		if !ok {
			break
		}
		f, err := n.Float64()
		if err != nil {
			break
		}
		nums = append(nums, f)
	}
	if len(nums) != len(vals) {
		return vals[0]
	}
	return combineNumbers(nums, attr)
}

func combineNumbers(nums []float64, attr string) json.RawMessage {
	acc := nums[0]
	switch mergeRuleFor(attr) {
	case ruleMin:
		for _, n := range nums[1:] {
			if n < acc {
				acc = n
			}
		}
	case ruleMax:
		for _, n := range nums[1:] {
			if n > acc {
				acc = n
			}
		}
	case ruleMean:
		for _, n := range nums[1:] {
			acc += n
		}
		acc /= float64(len(nums))
	default:
		for _, n := range nums[1:] {
			acc += n
		}
	}
	return json.RawMessage(strconv.FormatFloat(acc, 'f', -1, 64))
}

// decodedObject re-reads an object's fields as generic values so the whole thing
// can go through mergeMetricValue.
func decodedObject(obj map[string]json.RawMessage) any {
	out := make(map[string]any, len(obj))
	for k, raw := range obj {
		var v any
		if decodeJSON(raw, &v) == nil {
			out[k] = v
		}
	}
	return out
}

// decodeJSON keeps integers exact by decoding numbers as json.Number.
func decodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

// marshal reports ok=false rather than an error: an unmarshalable merge result
// has only one recovery, replaying a backend's body verbatim.
func marshal(v any) ([]byte, bool) {
	b, err := json.Marshal(v)
	return b, err == nil
}
