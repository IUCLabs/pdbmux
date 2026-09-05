package main

import (
	"context"
	"encoding/json"
	"errors"
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

	// Set by pdbmux, not by PuppetDB: how a cache-backed response was answered
	// and how old the served copy is.
	cacheStatusHeader = "X-Cache"
	ageHeader         = "Age"
)

type backendResult struct {
	name    string
	records []record
	total   int // upstream X-Records count, or -1 when the backend sent none
	err     error
}

var errAllBackendsFailed = errors.New("all backends failed")

type Server struct {
	cfg    Config
	client *http.Client
	log    *log.Logger

	// factsCache is nil when caching is disabled; cacheFor hands out a noop then.
	factsCache Cache
	flights    flightGroup
	stale      staleTracker

	// freshness cache (freshness merge only).
	mu        sync.Mutex
	freshData freshness
	freshAt   time.Time
}

func NewServer(cfg Config, logger *log.Logger) *Server {
	cfg.clampFactsTTL()
	s := &Server{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		log:    logger,
	}
	if cfg.cacheEnabled() {
		s.factsCache = newMemoryCache(cfg.FactsTTL, cfg.CacheBytes)
	}
	return s
}

// cacheFor picks the cache backing a request. Merged /facts and /nodes record
// sets share the in-memory cache; every other path is uncached until the reports
// cache lands, and a new backend is a case here rather than a change to any
// handler.
func (s *Server) cacheFor(path string, params url.Values) (Cache, bool) {
	switch path {
	case factsPath, nodesPath:
		// An aggregate row is a summed count, not the merged record set the
		// cache was built for, so it stays on the live path.
		if parseAggregate(params.Get("query")) != nil {
			return noopCache{}, false
		}
		if s.factsCache != nil {
			return s.factsCache, true
		}
	}
	return noopCache{}, false
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
	params := queryParams(r.URL.Query().Get("query"))
	s.serveCached(w, r, path, params, func(ctx context.Context) (cachedResponse, error) {
		alive, err := s.aliveResults(ctx, path, params)
		if err != nil {
			return cachedResponse{}, err
		}
		return cachedResponse{Body: encodeRecords(merge(alive)), Records: -1}, nil
	})
}

// Reports and events are immutable history, so both backends' records belong in the merged view.
func (s *Server) serveUnion(w http.ResponseWriter, r *http.Request, path string, key func(record) (string, bool)) {
	in := r.URL.Query()
	page, err := parsePaging(in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Keyed on the request's own params, not the upstream ones: upstreamParams
	// folds offset into limit, so different windows would collide.
	s.serveCached(w, r, path, in, func(ctx context.Context) (cachedResponse, error) {
		alive, err := s.aliveResults(ctx, path, page.upstreamParams(in))
		if err != nil {
			return cachedResponse{}, err
		}
		merged := mergeUnion(alive, key)
		sortRecords(merged, page.order)
		resp := cachedResponse{Body: encodeRecords(page.apply(merged)), Records: -1}
		if page.wantTotal {
			if total := sumTotals(alive); total >= 0 {
				resp.Records = total
			}
		}
		return resp, nil
	})
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

	s.serveCached(w, r, path, in, func(ctx context.Context) (cachedResponse, error) {
		alive, err := s.aliveResults(ctx, path, page.upstreamParams(in))
		if err != nil {
			return cachedResponse{}, err
		}
		merged := sumRows(alive, columns)
		sortRecords(merged, page.order)
		resp := cachedResponse{Body: encodeRecords(page.apply(merged)), Records: -1}
		if page.wantTotal {
			resp.Records = len(merged)
		}
		return resp, nil
	})
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

// Returns errAllBackendsFailed only when every backend failed.
func (s *Server) aliveResults(ctx context.Context, path string, params url.Values) ([]backendResult, error) {
	results := s.fanOut(ctx, path, params)

	var alive []backendResult
	for _, res := range results {
		if res.err != nil {
			s.log.Printf("warning: backend %q failed for %s: %v", res.name, path, res.err)
			continue
		}
		alive = append(alive, res)
	}
	if len(alive) == 0 {
		return nil, errAllBackendsFailed
	}
	return alive, nil
}

// cachedResponse is the stored form of a merged response: the JSON body plus the
// X-Records value it carried, so a cache hit reproduces both.
type cachedResponse struct {
	Body    json.RawMessage `json:"body"`
	Records int             `json:"records"` // -1 when the response sets no X-Records
}

// serveCached answers from the cache when the entry is fresh, otherwise runs
// build — single-flighted, so N concurrent identical requests cause one upstream
// fan-out — and stores the result. A build failure falls back to a stale entry
// when one exists; that is the only path on which stale data is served. Paths
// with no cache configured run build directly, unchanged.
func (s *Server) serveCached(w http.ResponseWriter, r *http.Request, path string, params url.Values, build func(context.Context) (cachedResponse, error)) {
	cache, enabled := s.cacheFor(path, params)
	if !enabled {
		resp, err := build(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeCached(w, resp)
		return
	}

	key := cacheKey(path, params)
	var stale *CacheEntry
	ent, status, err := cache.Get(r.Context(), key)
	switch {
	case err != nil:
		s.log.Printf("warning: cache lookup for %s failed: %v", key, err)
	case status == CacheFresh:
		s.stale.markFresh()
		s.writeStored(w, ent, CacheFresh)
		return
	case status == CacheStale:
		stale = &ent
	}

	// The flight is shared, so it runs on a context detached from whichever
	// request happened to lead it: one client disconnecting must not cancel the
	// fan-out its followers are waiting on. cfg.Timeout keeps it bounded.
	resp, err, _ := s.flights.Do(r.Context(), key, func() (cachedResponse, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), s.flightTimeout())
		defer cancel()

		built, buildErr := build(ctx)
		if buildErr != nil {
			return cachedResponse{}, buildErr
		}
		body, marshalErr := json.Marshal(built)
		if marshalErr != nil {
			s.log.Printf("warning: encoding cache entry for %s failed: %v", key, marshalErr)
			return built, nil
		}
		if putErr := cache.Put(ctx, key, body); putErr != nil {
			s.log.Printf("warning: cache store for %s failed: %v", key, putErr)
		}
		return built, nil
	})
	if err != nil {
		// This caller abandoned the flight because its own client went away; the
		// flight itself is still running for everyone else and there is nobody
		// left to write to.
		if rerr := r.Context().Err(); rerr != nil && errors.Is(err, rerr) {
			return
		}
		if stale != nil {
			s.stale.markStale(time.Now())
			s.log.Printf("warning: serving stale %s from cache (stored %s): %v",
				path, stale.StoredAt.UTC().Format(time.RFC3339), err)
			s.writeStored(w, *stale, CacheStale)
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	s.stale.markFresh()
	setCacheHeaders(w, CacheMiss, time.Time{})
	writeCached(w, resp)
}

// http.Client reads a zero Timeout as "no deadline", but it would expire a
// context immediately, so an unset value falls back to the default.
func (s *Server) flightTimeout() time.Duration {
	if s.cfg.Timeout > 0 {
		return s.cfg.Timeout
	}
	return defaultTimeout
}

func (s *Server) writeStored(w http.ResponseWriter, ent CacheEntry, status CacheStatus) {
	var resp cachedResponse
	if err := json.Unmarshal(ent.Body, &resp); err != nil {
		s.log.Printf("warning: unreadable cache entry: %v", err)
		http.Error(w, "unreadable cache entry", http.StatusBadGateway)
		return
	}
	setCacheHeaders(w, status, ent.StoredAt)
	writeCached(w, resp)
}

// setCacheHeaders labels a response from a cache-backed path: X-Cache is
// hit/stale/miss and Age is whole seconds since the served copy was stored (0
// for a response built by this request).
func setCacheHeaders(w http.ResponseWriter, status CacheStatus, storedAt time.Time) {
	label := "miss"
	switch status {
	case CacheFresh:
		label = "hit"
	case CacheStale:
		label = "stale"
	}
	age := 0
	if !storedAt.IsZero() {
		if secs := int(time.Since(storedAt).Seconds()); secs > 0 {
			age = secs
		}
	}
	w.Header().Set(cacheStatusHeader, label)
	w.Header().Set(ageHeader, strconv.Itoa(age))
}

func writeCached(w http.ResponseWriter, resp cachedResponse) {
	w.Header().Set("Content-Type", "application/json")
	if resp.Records >= 0 {
		w.Header().Set(recordsHeader, strconv.Itoa(resp.Records))
	}
	// resp.Body is shared with the cache and with every caller of a single
	// flight, so it is written, never appended to.
	body := []byte(resp.Body)
	if len(body) == 0 {
		body = []byte("[]")
	}
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

func encodeRecords(recs []json.RawMessage) json.RawMessage {
	if recs == nil {
		recs = []json.RawMessage{}
	}
	b, err := json.Marshal(recs)
	if err != nil {
		return json.RawMessage("[]")
	}
	return b
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
	Cache    cacheHealth       `json:"cache"`
}

type cacheHealth struct {
	Backend      string `json:"backend"` // "memory" | "none"
	TTL          string `json:"ttl"`
	Entries      int    `json:"entries"`
	StaleEntries int    `json:"stale_entries"` // cached entries past their TTL
	Bytes        int64  `json:"bytes"`
	ServingStale bool   `json:"serving_stale"` // last cached response came from a stale entry
	StaleServed  uint64 `json:"stale_served"`
	LastStale    string `json:"last_stale_served,omitempty"`
}

func (s *Server) cacheHealth() cacheHealth {
	stats := CacheStats{Backend: "none"}
	ttl := time.Duration(0)
	if s.factsCache != nil {
		stats = s.factsCache.Stats()
		ttl = s.cfg.FactsTTL
	}
	serving, served, last := s.stale.snapshot()
	h := cacheHealth{
		Backend:      stats.Backend,
		TTL:          durationString(ttl),
		Entries:      stats.Entries,
		StaleEntries: stats.StaleEntries,
		Bytes:        stats.Bytes,
		ServingStale: serving,
		StaleServed:  served,
	}
	if !last.IsZero() {
		h.LastStale = last.UTC().Format(time.RFC3339)
	}
	return h
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	probe := `["=","certname","pdbmux-healthz-probe"]`
	results := s.fanOut(r.Context(), nodesPath, queryParams(probe))

	report := healthReport{Backends: map[string]string{}, Cache: s.cacheHealth()}
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
