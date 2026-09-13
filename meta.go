package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

const (
	metaVersionPath    = "/pdb/meta/v1/version"
	metaServerTimePath = "/pdb/meta/v1/server-time"
)

// rawResult is one backend's verbatim response, for endpoints whose payload is
// not a PuppetDB record array.
type rawResult struct {
	name        string
	status      int
	contentType string
	body        []byte
	err         error
}

// ok reports whether the backend answered 2xx.
func (r rawResult) ok() bool {
	return r.err == nil && r.status >= 200 && r.status < 300
}

// fanOutRaw asks every backend for path concurrently and returns one result per
// backend, in configured order, without interpreting the bodies.
func (s *Server) fanOutRaw(ctx context.Context, path, rawQuery string) []rawResult {
	results := make([]rawResult, len(s.cfg.Backends))
	var wg sync.WaitGroup
	for i, b := range s.cfg.Backends {
		wg.Add(1)
		go func(i int, b Backend) {
			defer wg.Done()
			results[i] = s.rawBackend(ctx, b, path, rawQuery)
		}(i, b)
	}
	wg.Wait()
	return results
}

func (s *Server) rawBackend(ctx context.Context, b Backend, path, rawQuery string) rawResult {
	target := strings.TrimRight(b.URL, "/") + path
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return rawResult{name: b.Name, err: err}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return rawResult{name: b.Name, err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return rawResult{name: b.Name, err: err}
	}
	return rawResult{
		name:        b.Name,
		status:      resp.StatusCode,
		contentType: resp.Header.Get("Content-Type"),
		body:        body,
	}
}

// aliveRaw drops backends that errored or answered non-2xx, returning ok=false
// when none is left. The reply it writes then is the same one the merged query
// handlers write, so /pdb/meta and /metrics answer a failure exactly as
// /pdb/query does: a unanimous upstream error replayed, anything else a 502.
func (s *Server) aliveRaw(w http.ResponseWriter, results []rawResult, path string) ([]rawResult, bool) {
	var alive []rawResult
	for _, res := range results {
		if !res.ok() {
			s.log.Printf("warning: backend %q failed for %s: %s", res.name, path, res.reason())
			continue
		}
		alive = append(alive, res)
	}
	if len(alive) == 0 {
		s.writeUpstreamError(w, upstreamOutcome(rawUpstreamErrors(results)))
		return nil, false
	}
	return alive, true
}

// rawUpstreamErrors reduces a raw fan-out to one entry per backend, nil where
// the backend answered 2xx or never answered at all.
func rawUpstreamErrors(results []rawResult) []*upstreamError {
	errs := make([]*upstreamError, len(results))
	for i, res := range results {
		if res.err != nil || res.ok() {
			continue
		}
		errs[i] = newUpstreamError(res.status, res.contentType, res.body)
	}
	return errs
}

func (r rawResult) reason() string {
	if r.err != nil {
		return r.err.Error()
	}
	return "HTTP " + strconv.Itoa(r.status) + ": " + strings.TrimSpace(string(r.body))
}

// handleMetaVersion serves /pdb/meta/v1/version. Clients (pypuppetdb, and so
// Puppetboard's startup check) treat the answer as the feature level they may
// rely on, so the merged answer is the *lowest* version any backend reports:
// the estate can only be counted on for what its oldest member implements.
func (s *Server) handleMetaVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	alive, ok := s.aliveRaw(w, s.fanOutRaw(r.Context(), metaVersionPath, r.URL.RawQuery), metaVersionPath)
	if !ok {
		return
	}

	lowest := alive[0]
	lowestVer, hasVer := metaField(lowest.body, "version")
	for _, res := range alive[1:] {
		v, ok := metaField(res.body, "version")
		if !ok {
			continue
		}
		if !hasVer || compareVersions(v, lowestVer) < 0 {
			lowest, lowestVer, hasVer = res, v, true
		}
	}
	writeRaw(w, lowest)
}

// handleMetaServerTime serves /pdb/meta/v1/server-time. The clock of whichever
// PuppetDB answered is not estate state and does not merge, so the first
// reachable backend in configured order supplies it — the same tie-break rule
// used everywhere else.
func (s *Server) handleMetaServerTime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	alive, ok := s.aliveRaw(w, s.fanOutRaw(r.Context(), metaServerTimePath, r.URL.RawQuery), metaServerTimePath)
	if !ok {
		return
	}
	writeRaw(w, alive[0])
}

// metaField pulls a string field out of a `{"version": "..."}`-shaped body.
func metaField(body []byte, field string) (string, bool) {
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil {
		return "", false
	}
	var s string
	if json.Unmarshal(obj[field], &s) != nil || s == "" {
		return "", false
	}
	return s, true
}

// compareVersions orders dotted version strings segment by segment, comparing
// numerically where both segments are numbers and lexically otherwise, so
// "7.12.1" sorts below "8.4.0" and below "7.12.2". A prefix is lower than a
// longer string sharing it ("7.12" < "7.12.1"), and a pre-release suffix is
// compared as text within its segment ("8.0.0" < "8.0.0-SNAPSHOT").
func compareVersions(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		an, aok := strconv.Atoi(as[i])
		bn, bok := strconv.Atoi(bs[i])
		if aok == nil && bok == nil {
			if an != bn {
				return sign(an - bn)
			}
			continue
		}
		if c := strings.Compare(as[i], bs[i]); c != 0 {
			return c
		}
	}
	return sign(len(as) - len(bs))
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

func writeRaw(w http.ResponseWriter, res rawResult) {
	setContentType(w, res.contentType)
	w.WriteHeader(res.status)
	_, _ = w.Write(res.body)
}
