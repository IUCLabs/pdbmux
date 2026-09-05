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
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	factsPath   = "/pdb/query/v4/facts"
	nodesPath   = "/pdb/query/v4/nodes"
	reportsPath = "/pdb/query/v4/reports"
	eventsPath  = "/pdb/query/v4/events"
	queryV4     = "/pdb/query/v4/"

	// recordsHeader is PuppetDB's total-result-count header, returned when a
	// request carries include_total=true.
	recordsHeader = "X-Records"
)

// backendResult is one backend's decoded response for a query. err is non-nil
// when the backend failed (network/timeout/non-2xx); such results carry no
// records and are excluded from the merge but logged.
type backendResult struct {
	name    string
	records []record
	total   int // upstream X-Records count, or -1 when the backend sent none
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
// per node, /reports and /events are unioned across backends, a report's
// sub-resources resolve to whichever backend stores that report, and every other
// v4 path is transparently proxied to the primary.
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
	case reportsPath:
		s.serveUnion(w, r, reportsPath, reportKey)
	case eventsPath:
		s.serveUnion(w, r, eventsPath, rawKey)
	default:
		if isReportSubResource(r.URL.Path) {
			s.serveFirstHolder(w, r)
			return
		}
		s.proxyPrimary(w, r)
	}
}

// isReportSubResource reports whether path is a per-report child endpoint —
// /pdb/query/v4/reports/<hash>/{events,logs,metrics} — whose data lives in
// exactly one backend.
func isReportSubResource(path string) bool {
	rest, ok := strings.CutPrefix(path, reportsPath+"/")
	if !ok {
		return false
	}
	hash, sub, ok := strings.Cut(rest, "/")
	if !ok || hash == "" {
		return false
	}
	switch sub {
	case "events", "logs", "metrics":
		return true
	}
	return false
}

// serveMerged fans out the request to all backends, then hands the per-backend
// results to merge to produce the response body. If every backend fails it
// returns 502; if some fail it serves the survivors and logs a warning.
func (s *Server) serveMerged(w http.ResponseWriter, r *http.Request, path string, merge func([]backendResult) []json.RawMessage) {
	alive, ok := s.aliveResults(w, r, path, queryParams(r.URL.Query().Get("query")))
	if !ok {
		return
	}
	writeJSON(w, merge(alive))
}

// serveUnion fans out a request whose records are immutable history — reports
// and events — and serves the deduped union of every backend. Because each
// backend ordered and paged only its own slice, the union is re-ordered and
// re-paged here from the client's order_by/limit/offset.
func (s *Server) serveUnion(w http.ResponseWriter, r *http.Request, path string, key func(record) (string, bool)) {
	in := r.URL.Query()
	page, err := parsePaging(in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	alive, ok := s.aliveResults(w, r, path, page.upstreamParams(in))
	if !ok {
		return
	}

	merged := mergeUnion(s.byPrecedence(alive), key)
	sortRecords(merged, page.order)
	if page.wantTotal {
		if total := sumTotals(alive); total >= 0 {
			w.Header().Set(recordsHeader, strconv.Itoa(total))
		}
	}
	writeJSON(w, page.apply(merged))
}

// serveFirstHolder answers a per-report sub-resource request. The report lives
// in exactly one backend, so all are asked concurrently and the first one (in
// precedence order) that actually holds it wins. Backends that do not have the
// report answer 404, which is indistinguishable here from any other failure, so
// an empty result is only served once every backend has been consulted.
func (s *Server) serveFirstHolder(w http.ResponseWriter, r *http.Request) {
	results := s.fanOut(r.Context(), r.URL.Path, r.URL.Query())

	var alive []backendResult
	for _, res := range results {
		if res.err != nil {
			s.log.Printf("info: backend %q has no %s: %v", res.name, r.URL.Path, res.err)
			continue
		}
		alive = append(alive, res)
	}
	if len(alive) == 0 {
		http.Error(w, "no backend holds this report", http.StatusNotFound)
		return
	}
	for _, res := range s.byPrecedence(alive) {
		if len(res.records) > 0 {
			writeJSON(w, rawRecords(res.records))
			return
		}
	}
	writeJSON(w, nil)
}

// aliveResults fans out to every backend and returns the successful results.
// It writes a 502 and returns ok=false when every backend failed.
func (s *Server) aliveResults(w http.ResponseWriter, r *http.Request, path string, params url.Values) ([]backendResult, bool) {
	results := s.fanOut(r.Context(), path, params)

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
		return nil, false
	}
	return alive, true
}

// queryParams builds the upstream param set for a merged endpoint that only
// forwards the PuppetDB query.
func queryParams(query string) url.Values {
	if query == "" {
		return nil
	}
	return url.Values{"query": []string{query}}
}

// rawRecords strips decoded metadata back down to the verbatim JSON elements.
func rawRecords(recs []record) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Raw)
	}
	return out
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
	nodeResults := s.fanOut(ctx, nodesPath, nil)
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

// fanOut queries every backend concurrently for path with the given params and
// returns one backendResult per backend, in config order.
func (s *Server) fanOut(ctx context.Context, path string, params url.Values) []backendResult {
	results := make([]backendResult, len(s.cfg.Backends))
	var wg sync.WaitGroup
	for i, b := range s.cfg.Backends {
		wg.Add(1)
		go func(i int, b Backend) {
			defer wg.Done()
			recs, total, err := s.queryBackend(ctx, b, path, params)
			results[i] = backendResult{name: b.Name, records: recs, total: total, err: err}
		}(i, b)
	}
	wg.Wait()
	return results
}

// queryBackend performs one GET b.URL+path?params and decodes the JSON array.
// It also returns the upstream X-Records count, or -1 when the backend sent none.
func (s *Server) queryBackend(ctx context.Context, b Backend, path string, params url.Values) ([]record, int, error) {
	target := strings.TrimRight(b.URL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, -1, err
	}
	if len(params) > 0 {
		req.URL.RawQuery = params.Encode()
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, -1, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, -1, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, -1, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	total := -1
	if n, err := strconv.Atoi(resp.Header.Get(recordsHeader)); err == nil && n >= 0 {
		total = n
	}
	recs, err := decodeRecords(body)
	return recs, total, err
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
	results := s.fanOut(r.Context(), nodesPath, queryParams(probe))

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
