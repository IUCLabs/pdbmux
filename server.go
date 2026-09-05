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

	// PuppetDB only sends this when the request carries include_total=true.
	recordsHeader = "X-Records"
)

type backendResult struct {
	name    string
	records []record
	total   int // upstream X-Records count, or -1 when the backend sent none
	err     error
}

type Server struct {
	cfg    Config
	client *http.Client
	log    *log.Logger

	// freshness cache (freshness merge only).
	mu        sync.Mutex
	freshData freshness
	freshAt   time.Time
}

func NewServer(cfg Config, logger *log.Logger) *Server {
	return &Server{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		log:    logger,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/pdb/query/v4/", s.handleQuery)
	return mux
}

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

// Matches /pdb/query/v4/reports/<hash>/{events,logs,metrics}, whose data lives in exactly one backend.
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

func (s *Server) serveMerged(w http.ResponseWriter, r *http.Request, path string, merge func([]backendResult) []json.RawMessage) {
	alive, ok := s.aliveResults(w, r, path, queryParams(r.URL.Query().Get("query")))
	if !ok {
		return
	}
	writeJSON(w, merge(alive))
}

// Reports and events are immutable history, so both backends' records belong in the merged view.
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

// A backend without the report answers 404, indistinguishable from a failure, so every backend is consulted before serving empty.
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

// Writes a 502 and returns ok=false only when every backend failed.
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

func queryParams(query string) url.Values {
	if query == "" {
		return nil
	}
	return url.Values{"query": []string{query}}
}

func rawRecords(recs []record) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Raw)
	}
	return out
}

func (s *Server) mergeNodesResponse(results []backendResult) []json.RawMessage {
	return mergeNodes(s.byPrecedence(results))
}

func (s *Server) mergeFactsResponse(results []backendResult) []json.RawMessage {
	ordered := s.byPrecedence(results)
	if s.cfg.Merge == mergeStatic {
		prefer := s.cfg.Prefer
		return mergeFacts(ordered, func(string) string { return prefer })
	}
	fresh := s.freshnessMap(context.Background(), ordered)
	prefer := s.cfg.Prefer
	return mergeFacts(ordered, func(cn string) string {
		if b, ok := fresh[cn]; ok {
			return b
		}
		return prefer
	})
}

// Puts Prefer first so it wins ties; the rest keep config order.
func (s *Server) byPrecedence(results []backendResult) []backendResult {
	ordered := make([]backendResult, len(results))
	copy(ordered, results)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].name == s.cfg.Prefer && ordered[j].name != s.cfg.Prefer
	})
	return ordered
}

// Queries /nodes unfiltered rather than reusing the request's results, because a /facts query's certname set can differ.
func (s *Server) freshnessMap(ctx context.Context, _ []backendResult) freshness {
	s.mu.Lock()
	if s.freshData != nil && time.Since(s.freshAt) < s.cfg.FreshnessTTL {
		f := s.freshData
		s.mu.Unlock()
		return f
	}
	s.mu.Unlock()

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

// Returns one result per backend, in config order.
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

// The returned count is the upstream X-Records value, or -1 when the backend sent none.
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

type healthReport struct {
	Status   string            `json:"status"`
	Backends map[string]string `json:"backends"` // name -> "ok" | error text
}

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

func writeJSON(w http.ResponseWriter, recs []json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	if recs == nil {
		recs = []json.RawMessage{}
	}
	_ = json.NewEncoder(w).Encode(recs)
}
