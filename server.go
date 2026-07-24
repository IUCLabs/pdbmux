package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	factsPath = "/pdb/query/v4/facts"
	nodesPath = "/pdb/query/v4/nodes"
	queryV4   = "/pdb/query/v4/"
)

// backendResult is one backend's decoded response for a query. err is non-nil
// when the backend failed (network/timeout/non-2xx); such results carry no
// records and are excluded from the merge but logged.
type backendResult struct {
	name    string
	records []record
	err     error
}

// Server proxies and merges PuppetDB queries across the configured backends.
type Server struct {
	cfg    Config
	client *http.Client
	log    *log.Logger

	// freshness cache (freshness merge only).
	mu        sync.Mutex
	freshData freshness
	freshAt   time.Time
}

// NewServer builds a Server with an HTTP client bounded by cfg.Timeout.
func NewServer(cfg Config, logger *log.Logger) *Server {
	return &Server{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		log:    logger,
	}
}

// Handler returns the HTTP mux for the proxy.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/pdb/query/v4/", s.handleQuery)
	return mux
}

// handleQuery dispatches /pdb/query/v4/* requests: /facts and /nodes are merged
// across backends; every other v4 path is transparently proxied to the primary.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	switch r.URL.Path {
	case nodesPath:
		s.serveMerged(w, r, nodesPath, s.mergeNodesResponse)
	case factsPath:
		s.serveMerged(w, r, factsPath, s.mergeFactsResponse)
	default:
		s.proxyPrimary(w, r)
	}
}

// serveMerged fans out the request to all backends, then hands the per-backend
// results to merge to produce the response body. If every backend fails it
// returns 502; if some fail it serves the survivors and logs a warning.
func (s *Server) serveMerged(w http.ResponseWriter, r *http.Request, path string, merge func([]backendResult) []json.RawMessage) {
	query := r.URL.Query().Get("query")
	results := s.fanOut(r.Context(), path, query)

	var alive []backendResult
	for _, res := range results {
		if res.err != nil {
			s.log.Printf("warning: backend %q failed for %s: %v", res.name, path, res.err)
			continue
		}
		alive = append(alive, res)
	}
	if len(alive) == 0 {
		http.Error(w, "all backends failed", http.StatusBadGateway)
		return
	}

	merged := merge(alive)
	writeJSON(w, merged)
}

// mergeNodesResponse merges /nodes results (dedupe by certname, newer wins).
func (s *Server) mergeNodesResponse(results []backendResult) []json.RawMessage {
	return mergeNodes(s.byPrecedence(results))
}

// mergeFactsResponse merges /facts results at node granularity, choosing each
// certname's owner via the configured merge strategy.
func (s *Server) mergeFactsResponse(results []backendResult) []json.RawMessage {
	ordered := s.byPrecedence(results)
	if s.cfg.Merge == mergeStatic {
		prefer := s.cfg.Prefer
		return mergeFacts(ordered, func(string) string { return prefer })
	}
	// freshness merge: attribute each certname to the backend with the newer
	// report_timestamp, taken from a short-TTL /nodes freshness map.
	fresh := s.freshnessMap(context.Background(), ordered)
	prefer := s.cfg.Prefer
	return mergeFacts(ordered, func(cn string) string {
		if b, ok := fresh[cn]; ok {
			return b
		}
		return prefer
	})
}

// byPrecedence orders results so the Prefer backend comes first, giving it the
// tie-break on equal timestamps. Remaining backends keep config order.
func (s *Server) byPrecedence(results []backendResult) []backendResult {
	ordered := make([]backendResult, len(results))
	copy(ordered, results)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].name == s.cfg.Prefer && ordered[j].name != s.cfg.Prefer
	})
	return ordered
}

// freshnessMap returns a per-certname owner map derived from each backend's
// /nodes report_timestamp, cached for cfg.FreshnessTTL. On cache miss it queries
// /nodes from all backends; a backend that fails is simply absent from the map,
// so its certnames fall back to precedence/Prefer.
//
// When the incoming request already carries /nodes data (results has records),
// we still query /nodes broadly here because a /facts query's certname set can
// differ from what the request's query filter returned. The cache keeps this
// cheap under load.
func (s *Server) freshnessMap(ctx context.Context, _ []backendResult) freshness {
	s.mu.Lock()
	if s.freshData != nil && time.Since(s.freshAt) < s.cfg.FreshnessTTL {
		f := s.freshData
		s.mu.Unlock()
		return f
	}
	s.mu.Unlock()

	// Empty query = all nodes; cheap enough for a short-TTL cache.
	nodeResults := s.fanOut(ctx, nodesPath, "")
	var alive []backendResult
	for _, res := range nodeResults {
		if res.err != nil {
			s.log.Printf("warning: freshness /nodes query to %q failed: %v", res.name, res.err)
			continue
		}
		alive = append(alive, res)
	}
	f := buildFreshness(s.byPrecedence(alive))

	s.mu.Lock()
	s.freshData = f
	s.freshAt = time.Now()
	s.mu.Unlock()
	return f
}

// fanOut queries every backend concurrently for path?query=... and returns one
// backendResult per backend, in config order.
func (s *Server) fanOut(ctx context.Context, path, query string) []backendResult {
	results := make([]backendResult, len(s.cfg.Backends))
	var wg sync.WaitGroup
	for i, b := range s.cfg.Backends {
		wg.Add(1)
		go func(i int, b Backend) {
			defer wg.Done()
			recs, err := s.queryBackend(ctx, b, path, query)
			results[i] = backendResult{name: b.Name, records: recs, err: err}
		}(i, b)
	}
	wg.Wait()
	return results
}

// queryBackend performs one GET b.URL+path?query=... and decodes the JSON array.
func (s *Server) queryBackend(ctx context.Context, b Backend, path, query string) ([]record, error) {
	target := strings.TrimRight(b.URL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	if query != "" {
		q := url.Values{}
		q.Set("query", query)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return decodeRecords(body)
}

// proxyPrimary transparently forwards a non-merged /pdb/query/v4/* request to
// the primary backend and streams the response back verbatim.
func (s *Server) proxyPrimary(w http.ResponseWriter, r *http.Request) {
	b := s.cfg.PrimaryBackend()
	target := strings.TrimRight(b.URL, "/") + r.URL.Path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Printf("warning: primary %q pass-through failed for %s: %v", b.Name, r.URL.Path, err)
		http.Error(w, "primary backend failed", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// healthReport is the /healthz JSON body.
type healthReport struct {
	Status   string            `json:"status"`
	Backends map[string]string `json:"backends"` // name -> "ok" | error text
}

// handleHealth probes every backend's /nodes endpoint with a trivial query and
// reports per-backend reachability. Overall status is "ok" if any backend is
// reachable, "degraded" if some fail, "down" if all fail (503 in that case).
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	probe := `["=","certname","pdbmux-healthz-probe"]`
	results := s.fanOut(r.Context(), nodesPath, probe)

	report := healthReport{Backends: map[string]string{}}
	healthy := 0
	for _, res := range results {
		if res.err != nil {
			report.Backends[res.name] = res.err.Error()
			continue
		}
		report.Backends[res.name] = "ok"
		healthy++
	}
	switch {
	case healthy == len(results):
		report.Status = "ok"
	case healthy > 0:
		report.Status = "degraded"
	default:
		report.Status = "down"
	}

	w.Header().Set("Content-Type", "application/json")
	if healthy == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}

// writeJSON writes a JSON array of raw records as a PuppetDB-style response.
func writeJSON(w http.ResponseWriter, recs []json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	if recs == nil {
		recs = []json.RawMessage{}
	}
	_ = json.NewEncoder(w).Encode(recs)
}
