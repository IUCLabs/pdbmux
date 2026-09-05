package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	factsPath                = "/pdb/query/v4/facts"
	nodesPath                = "/pdb/query/v4/nodes"
	resourcesPath            = "/pdb/query/v4/resources"
	reportsPath              = "/pdb/query/v4/reports"
	eventsPath               = "/pdb/query/v4/events"
	eventCountsPath          = "/pdb/query/v4/event-counts"
	aggregateEventCountsPath = "/pdb/query/v4/aggregate-event-counts"
	queryV4                  = "/pdb/query/v4/"

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
	mux.HandleFunc(metaVersionPath, s.handleMetaVersion)
	mux.HandleFunc(metaServerTimePath, s.handleMetaServerTime)
	mux.HandleFunc(metricsPrefix, s.handleMetrics)
	return mux
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	switch r.URL.Path {
	case nodesPath:
		s.serveNodes(w, r)
	case resourcesPath:
		s.serveResources(w, r)
	case factsPath:
		s.serveMerged(w, r, factsPath, s.mergeFactsResponse(r))
	case reportsPath:
		s.serveReports(w, r)
	case eventsPath:
		s.serveUnion(w, r, eventsPath, rawKey)
	case eventCountsPath, aggregateEventCountsPath:
		s.serveSummed(w, r, r.URL.Path, inferredColumns)
	default:
		if isReportSubResource(r.URL.Path) {
			s.serveFirstHolder(w, r)
			return
		}
		s.proxyUnmerged(w, r)
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

	merged := mergeUnion(alive, key)
	sortRecords(merged, page.order)
	if page.wantTotal {
		if total := sumTotals(alive); total >= 0 {
			w.Header().Set(recordsHeader, strconv.Itoa(total))
		}
	}
	writeJSON(w, page.apply(merged))
}

// A count row carries no certname, so the certname-keyed merge would collapse every backend's count into one backend's; aggregates take the summing path instead.
func (s *Server) serveNodes(w http.ResponseWriter, r *http.Request) {
	if spec := parseAggregate(r.URL.Query().Get("query")); spec != nil {
		s.serveSummed(w, r, nodesPath, spec.columns)
		return
	}
	s.serveMerged(w, r, nodesPath, s.mergeNodesResponse(r))
}

// Only aggregates merge: a resource record has no cross-backend identity to dedupe on, so a plain query stays on the pass-through path.
func (s *Server) serveResources(w http.ResponseWriter, r *http.Request) {
	if spec := parseAggregate(r.URL.Query().Get("query")); spec != nil {
		s.serveSummed(w, r, resourcesPath, spec.columns)
		return
	}
	s.proxyUnmerged(w, r)
}

// An `extract` query with a `function` column returns synthetic aggregate rows that carry no identity, so they are summed rather than unioned.
func (s *Server) serveReports(w http.ResponseWriter, r *http.Request) {
	if spec := parseAggregate(r.URL.Query().Get("query")); spec != nil {
		s.serveSummed(w, r, reportsPath, spec.columns)
		return
	}
	s.serveUnion(w, r, reportsPath, reportKey)
}

// Merged rows are fewer than the backends' combined records, so include_total reports the merged count rather than a sum of X-Records.
func (s *Server) serveSummed(w http.ResponseWriter, r *http.Request, path string, columns func(map[string]json.RawMessage) ([]string, []string)) {
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

	merged := sumRows(alive, columns)
	sortRecords(merged, page.order)
	if page.wantTotal {
		w.Header().Set(recordsHeader, strconv.Itoa(len(merged)))
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
	for _, res := range alive {
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

func (s *Server) mergeNodesResponse(r *http.Request) func([]backendResult) []json.RawMessage {
	inject := s.newSourceInjector(r.URL.Query().Get("query"), false)
	return func(results []backendResult) []json.RawMessage {
		return mergeNodes(results, inject)
	}
}

func (s *Server) mergeFactsResponse(r *http.Request) func([]backendResult) []json.RawMessage {
	inject := s.newSourceInjector(r.URL.Query().Get("query"), true)
	return func(results []backendResult) []json.RawMessage {
		var merged []json.RawMessage
		if s.cfg.Merge == mergeStatic {
			merged = mergeFacts(results, nil, inject)
		} else {
			fresh := s.freshnessMap(context.Background(), results)
			merged = mergeFacts(results, func(cn string) string { return fresh[cn] }, inject)
		}
		inject.logSuppressed(s.log)
		return merged
	}
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
	f := buildFreshness(alive)

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

// The record shape is unknown, so a union would be guesswork: the first 2xx wins and the first error response is replayed when none succeeds.
func (s *Server) proxyUnmerged(w http.ResponseWriter, r *http.Request) {
	var fallback *bufferedResponse
	for _, b := range s.cfg.Backends {
		resp, err := s.passThrough(r, b)
		if err != nil {
			s.log.Printf("warning: backend %q pass-through failed for %s: %v", b.Name, r.URL.Path, err)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			setContentType(w, resp.Header.Get("Content-Type"))
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			_ = resp.Body.Close()
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if fallback == nil {
			fallback = &bufferedResponse{
				status:      resp.StatusCode,
				contentType: resp.Header.Get("Content-Type"),
				body:        body,
			}
		}
	}
	if fallback == nil {
		http.Error(w, "all backends failed", http.StatusBadGateway)
		return
	}
	setContentType(w, fallback.contentType)
	w.WriteHeader(fallback.status)
	_, _ = w.Write(fallback.body)
}

type bufferedResponse struct {
	status      int
	contentType string
	body        []byte
}

func (s *Server) passThrough(r *http.Request, b Backend) (*http.Response, error) {
	target := strings.TrimRight(b.URL, "/") + r.URL.Path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	return s.client.Do(req)
}

func setContentType(w http.ResponseWriter, contentType string) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
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
