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
	factNamesPath            = "/pdb/query/v4/fact-names"
	nodesPath                = "/pdb/query/v4/nodes"
	resourcesPath            = "/pdb/query/v4/resources"
	reportsPath              = "/pdb/query/v4/reports"
	eventsPath               = "/pdb/query/v4/events"
	eventCountsPath          = "/pdb/query/v4/event-counts"
	aggregateEventCountsPath = "/pdb/query/v4/aggregate-event-counts"
	queryV4                  = "/pdb/query/v4/"

	// The only column /fact-names projects, so the only one it can be ordered on.
	factNamesColumn = "name"

	// PuppetDB only sends this when the request carries include_total=true.
	recordsHeader = "X-Records"

	// Set by pdbmux, not by PuppetDB: how a cache-backed response was answered
	// and how old the served copy is.
	cacheStatusHeader = "X-Cache"
	ageHeader         = "Age"

	// Set by pdbmux: "<contributed>/<configured>" backends behind a merged response.
	backendsHeader = "X-Backends"
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

	// health is nil when probing is disabled, which makes every backend healthy.
	health  *prober
	partial partialTracker

	// now is shared with the cache's clock so Age matches the stored timestamp.
	now func() time.Time

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
		now:    time.Now,
	}
	if cfg.cacheEnabled() {
		s.factsCache = newMemoryCache(cfg.FactsTTL, cfg.CacheBytes)
	}
	s.health = newProber(cfg, logger)
	return s
}

// StartProbes begins background health probing; it never blocks on a first
// round, so the listener serves straight away.
func (s *Server) StartProbes(ctx context.Context) { s.health.Start(ctx) }

// StopProbes stops the probing goroutines and waits for them to exit.
func (s *Server) StopProbes() { s.health.Stop() }

// cacheFor picks the cache backing a request. The merged /nodes and fact record
// sets — /facts, /facts/<name>[/<value>] and /fact-names — share the in-memory
// cache; every other path is uncached until the reports cache lands, and a new
// backend is a case here rather than a change to any handler.
func (s *Server) cacheFor(path string, params url.Values) (Cache, bool) {
	switch {
	case path == factsPath, path == nodesPath, path == factNamesPath, isFactsSubPath(path):
		// An aggregate row is a combined count, not the merged record set the
		// cache was built for, so it stays on the live path.
		if spec, err := parseAggregate(params.Get("query")); spec != nil || err != nil {
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

// route is one query endpoint: how a request selects it, the path its fan-out
// asks backends for, and how it answers a plain query. handleQuery diverts an
// extract/function query to serveCombined before serve runs, so aggregate rows —
// which carry none of the certname, hash or name the merges key on, and would
// collapse into one backend's numbers — cannot reach an identity-keyed merge.
// unsummed opts a route out and records why; the zero value is guarded, so a
// route added without a decision is combined rather than silently merged.
type route struct {
	name    string
	matches func(path string) bool
	// fanOut is the path backends are queried on when the guard combines; empty
	// means the request's own path.
	fanOut   string
	serve    func(*Server, http.ResponseWriter, *http.Request)
	unsummed string
}

func pathIs(p string) func(string) bool {
	return func(path string) bool { return path == p }
}

var queryRoutes = []route{
	{name: nodesPath, matches: pathIs(nodesPath), fanOut: nodesPath, serve: (*Server).serveNodes},
	{name: factsPath, matches: pathIs(factsPath), fanOut: factsPath, serve: (*Server).serveFacts},
	// /resources has no cross-backend record identity, so only its aggregates merge.
	{name: resourcesPath, matches: pathIs(resourcesPath), fanOut: resourcesPath, serve: (*Server).proxyUnmerged},
	{name: reportsPath, matches: pathIs(reportsPath), fanOut: reportsPath, serve: (*Server).serveReports},
	{name: eventsPath, matches: pathIs(eventsPath), fanOut: eventsPath, serve: (*Server).serveEvents},
	{name: factsPath + "/<name>", matches: isFactsSubPath, serve: (*Server).serveFactsByName},
	{
		name:     factNamesPath,
		matches:  pathIs(factNamesPath),
		serve:    (*Server).serveFactNames,
		unsummed: "the union dedupes names across backends, so a count of it is not the sum of the backends' counts",
	},
	{
		name:     eventCountsPath,
		matches:  pathIs(eventCountsPath),
		serve:    (*Server).serveEventCounts,
		unsummed: "already summed, on columns inferred from the row rather than from the query",
	},
	{
		name:     aggregateEventCountsPath,
		matches:  pathIs(aggregateEventCountsPath),
		serve:    (*Server).serveEventCounts,
		unsummed: "already summed, on columns inferred from the row rather than from the query",
	},
	{
		name:     reportsPath + "/<hash>/<sub>",
		matches:  isReportSubResource,
		serve:    (*Server).serveFirstHolder,
		unsummed: "one backend holds the report, so nothing is merged across backends",
	},
}

// unmergedRoute answers every path no merged route claims.
var unmergedRoute = route{
	name:     "pass-through",
	serve:    (*Server).proxyUnmerged,
	unsummed: "one backend's response, verbatim; nothing is merged across backends",
}

func routeFor(path string) route {
	for _, rt := range queryRoutes {
		if rt.matches(path) {
			return rt
		}
	}
	return unmergedRoute
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	rt := routeFor(r.URL.Path)
	if rt.unsummed == "" {
		// The one place an aggregate is read, so a query pdbmux cannot fold is
		// refused here rather than by whichever handler happens to notice.
		in := r.URL.Query()
		spec, err := parseAggregate(in.Get("query"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if spec != nil {
			// Of the routes that reach an aggregate, only /events honours
			// distinct_resources; every other one rejects it as an unsupported
			// query parameter (src/puppetlabs/puppetdb/http/handlers.clj:178-189,
			// 475-496 and src/puppetlabs/puppetdb/http/query.clj:273-277).
			if r.URL.Path == eventsPath && distinctResources(in) {
				http.Error(w, errDistinctResourcesAggregate.Error(), http.StatusBadRequest)
				return
			}
			path := rt.fanOut
			if path == "" {
				path = r.URL.Path
			}
			s.serveCombined(w, r, path, spec, spec.shape)
			return
		}
	}
	rt.serve(s, w, r)
}

// factsSubPath matches /pdb/query/v4/facts/<name> and /pdb/query/v4/facts/<name>/<value>,
// returning the fact name the path constrains, the value it pins and whether it
// pins one at all. openvoxdb serves both from the facts entity, ANDing
// ["=","name",<name>] (and ["=","value",<value>]) onto the request's own query,
// so the record shape is exactly /facts' —
// src/puppetlabs/puppetdb/http/handlers.clj:283-302 and
// src/puppetlabs/puppetdb/http/query.clj:136-143,193-209.
func factsSubPath(path string) (name, value string, valued bool) {
	rest, ok := strings.CutPrefix(path, factsPath+"/")
	if !ok {
		return "", "", false
	}
	name, value, hasValue := strings.Cut(rest, "/")
	if name == "" {
		return "", "", false
	}
	if hasValue && (value == "" || strings.Contains(value, "/")) {
		return "", "", false
	}
	if !hasValue {
		value = ""
	}
	return name, value, hasValue
}

func isFactsSubPath(path string) bool {
	name, _, _ := factsSubPath(path)
	return name != ""
}

// /fact-names answers with a flat array of fact-name strings rather than
// certname-keyed records, so it gets its own union: dedupe by name and re-sort,
// because each backend only ordered its own slice. openvoxdb projects a single
// DISTINCT `name` column and defaults to name-ascending
// (src/puppetlabs/puppetdb/query_eng/engine.clj:488-498,
// src/puppetlabs/puppetdb/http/handlers.clj:349-362), so an order_by on any
// other field is rejected as openvoxdb rejects it, and the direction is all this
// route reads.
func (s *Server) serveFactNames(w http.ResponseWriter, r *http.Request) {
	in := r.URL.Query()
	page, err := parsePaging(in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, f := range page.order {
		if f.Field != factNamesColumn {
			http.Error(w, fmt.Sprintf("order_by field must be %q, got %q", factNamesColumn, f.Field), http.StatusBadRequest)
			return
		}
	}
	desc := len(page.order) > 0 && page.order[0].Desc

	// The merged /facts response carries the injected fact, so the list of names
	// has to carry its name; nothing produces it while injection is off.
	owned := ""
	if s.cfg.SourceFactEnabled {
		owned = s.cfg.SourceFact
	}

	upstream := page.upstreamParams(in)
	if page.wantTotal {
		// A merged total counts the whole union, so the window cannot be pushed
		// upstream; the full name list is small enough to fetch.
		upstream.Del("limit")
	}

	s.serveCached(w, r, factNamesPath, in, func(ctx context.Context) (cachedResponse, error) {
		alive, err := s.aliveResults(ctx, factNamesPath, upstream)
		if err != nil {
			return cachedResponse{}, err
		}
		merged := mergeFactNames(alive, owned, desc)
		resp := cachedResponse{Body: encodeRecords(page.apply(merged)), Records: -1}
		s.countBackends(&resp, alive)
		if page.wantTotal {
			resp.Records = len(merged)
		}
		return resp, nil
	})
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
		resp := cachedResponse{Body: encodeRecords(merge(alive)), Records: -1}
		s.countBackends(&resp, alive)
		return resp, nil
	})
}

// countBackends stamps a response with how many backends it was built from, of
// how many configured.
func (s *Server) countBackends(resp *cachedResponse, alive []backendResult) {
	resp.Backends, resp.Configured = len(alive), len(s.cfg.Backends)
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
		s.countBackends(&resp, alive)
		if page.wantTotal {
			if total := sumTotals(alive); total >= 0 {
				resp.Records = total
			}
		}
		return resp, nil
	})
}

func (s *Server) serveFacts(w http.ResponseWriter, r *http.Request) {
	s.serveMerged(w, r, factsPath, s.mergeFactsResponse(r))
}

// Both path forms are the facts entity with a name (and value) constraint ANDed
// on, so they take the same per-certname merge as /facts. A plain query on the
// source fact's own path is the one name no backend can answer for, so it is
// synthesised instead of fanned out as-is.
func (s *Server) serveFactsByName(w http.ResponseWriter, r *http.Request) {
	name, value, valued := factsSubPath(r.URL.Path)
	if inject := s.newSourceInjector(r.URL.Query().Get("query"), true); inject.claims(name) && inject.injects() {
		s.serveSourceFact(w, r, inject, value, valued)
		return
	}
	s.serveMerged(w, r, r.URL.Path, s.mergeFactsByNameResponse(r))
}

// serveSourceFact answers the drilldown on pdbmux's own fact. No backend holds a
// record of that name, so the route's own fan-out would return nothing; the
// records are taken from the /facts merge that produces them instead, which
// makes the certname set, the owner and the environment identical to the ones an
// unfiltered /facts response reports, and lets the request's query narrow the
// result upstream.
//
// That merge is the widest query pdbmux makes and the pinned value is
// client-supplied, so the value never reaches it: a value naming no configured
// backend is answered empty without any fan-out, and a value naming one filters
// a record set fetched under a value-independent key. Otherwise each distinct
// value would be a fresh cache key, a fresh flight and a fresh whole-estate
// fan-out.
func (s *Server) serveSourceFact(w http.ResponseWriter, r *http.Request, inject *sourceInjector, value string, valued bool) {
	// The synthetic record's value is always a backend name, so any other value
	// matches zero records. The response is complete rather than degraded, so it
	// reports every configured backend.
	if valued && !s.hasBackend(value) {
		n := len(s.cfg.Backends)
		writeCached(w, cachedResponse{Records: -1, Backends: n, Configured: n})
		return
	}

	params := queryParams(r.URL.Query().Get("query"))
	merge := s.mergeFactsWith(inject)
	var filter recordFilter
	if valued {
		filter = func(recs []json.RawMessage) []json.RawMessage {
			return sourceFactRecords(recs, inject.name, value, true)
		}
	}
	// The stored set is the whole owned-fact record set, so every value of it
	// keys, and waits on, the same fetch.
	s.serveFiltered(w, r, factsPath+"/"+inject.name, params, filter, func(ctx context.Context) (cachedResponse, error) {
		alive, err := s.aliveResults(ctx, factsPath, params)
		if err != nil {
			return cachedResponse{}, err
		}
		recs := sourceFactRecords(merge(alive), inject.name, "", false)
		resp := cachedResponse{Body: encodeRecords(recs), Records: -1}
		s.countBackends(&resp, alive)
		return resp, nil
	})
}

func (s *Server) hasBackend(name string) bool {
	for _, b := range s.cfg.Backends {
		if b.Name == name {
			return true
		}
	}
	return false
}

func (s *Server) serveNodes(w http.ResponseWriter, r *http.Request) {
	s.serveMerged(w, r, nodesPath, s.mergeNodesResponse(r))
}

func (s *Server) serveReports(w http.ResponseWriter, r *http.Request) {
	s.serveUnion(w, r, reportsPath, reportKey)
}

// Only a plain query reaches here: openvoxdb serves events from the same generic
// query engine as every other entity, so an extract carrying a ["function", ...]
// column is an aggregate handleQuery has already diverted —
// src/puppetlabs/puppetdb/http/handlers.clj:178-189 and
// src/puppetlabs/puppetdb/query_eng/engine.clj:1120-1210,1889-1911,2719-2733.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	s.serveUnion(w, r, eventsPath, rawKey)
}

// The columns come from the row rather than a query spec, so there is no
// grouping key or rewrite for serveCombined to apply.
func (s *Server) serveEventCounts(w http.ResponseWriter, r *http.Request) {
	s.serveCombined(w, r, r.URL.Path, nil, inferredShape)
}

// Merged rows are fewer than the backends' combined records, so include_total reports the merged count rather than a sum of X-Records.
func (s *Server) serveCombined(w http.ResponseWriter, r *http.Request, path string, spec *aggregateSpec, shape func(map[string]json.RawMessage) rowShape) {
	in := r.URL.Query()
	page, err := parsePaging(in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	upstream := page.upstreamParams(in)
	if spec != nil && len(spec.aggs) > 0 {
		// An aggregate returns one row per distinct group, so the whole result is
		// fetched and paged locally rather than truncated per backend.
		upstream = unpagedParams(in)
	}
	if spec != nil && spec.query != "" {
		// The backends answer the rewritten query, so they no longer carry the
		// column the client's order_by may name; the merged rows are sorted here.
		upstream.Set("query", spec.query)
		dropOrderBy(upstream, avgColumn)
	}

	s.serveCached(w, r, path, in, func(ctx context.Context) (cachedResponse, error) {
		alive, err := s.aliveResults(ctx, path, upstream)
		if err != nil {
			return cachedResponse{}, err
		}
		merged := combineRows(alive, shape)
		sortRecords(merged, page.order)
		resp := cachedResponse{Body: encodeRecords(page.apply(merged)), Records: -1}
		s.countBackends(&resp, alive)
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
		if ue := unanimousClientError(results); ue != nil {
			s.writeUpstreamError(w, ue)
			return
		}
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

// Returns an *upstreamError when every backend refused the query the same way,
// and errAllBackendsFailed when every backend failed for any other reason.
func (s *Server) aliveResults(ctx context.Context, path string, params url.Values) ([]backendResult, error) {
	results := s.fanOut(ctx, path, params)

	var alive []backendResult
	for _, res := range results {
		if res.err == nil {
			alive = append(alive, res)
		}
	}
	if len(alive) == 0 {
		// A query every backend refuses identically is the client's mistake, not an
		// outage, so it is neither logged as one nor counted as degraded service.
		if ue := unanimousClientError(results); ue != nil {
			s.log.Printf("info: every backend refused %s: %v", path, ue)
			return nil, ue
		}
	}
	for _, res := range results {
		if res.err != nil {
			s.log.Printf("warning: backend %q failed for %s: %v", res.name, path, res.err)
		}
	}
	s.partial.record(len(alive), len(s.cfg.Backends), s.now())
	if len(alive) == 0 {
		return nil, errAllBackendsFailed
	}
	return alive, nil
}

// cachedResponse is the stored form of a merged response: the JSON body, the
// X-Records value it carried and how many backends it was built from, so a cache
// hit reproduces all three.
type cachedResponse struct {
	Body       json.RawMessage `json:"body"`
	Records    int             `json:"records"`    // -1 when the response sets no X-Records
	Backends   int             `json:"backends"`   // backends that contributed records
	Configured int             `json:"configured"` // backends configured at build time
}

// recordFilter narrows a response's records after it has been built or read back
// from the cache, so requests differing only in the filter share one stored entry
// and one fan-out. It leaves Records alone, so it only suits responses that set
// no X-Records.
type recordFilter func([]json.RawMessage) []json.RawMessage

func (f recordFilter) apply(resp cachedResponse) cachedResponse {
	if f == nil {
		return resp
	}
	var recs []json.RawMessage
	if json.Unmarshal(resp.Body, &recs) != nil {
		return resp
	}
	resp.Body = encodeRecords(f(recs))
	return resp
}

// serveCached answers from the cache when the entry is fresh, otherwise runs
// build — single-flighted, so N concurrent identical requests cause one upstream
// fan-out — and stores the result. A build failure falls back to a stale entry
// when one exists; that is the only path on which stale data is served. Paths
// with no cache configured run build directly, unchanged.
func (s *Server) serveCached(w http.ResponseWriter, r *http.Request, path string, params url.Values, build func(context.Context) (cachedResponse, error)) {
	s.serveFiltered(w, r, path, params, nil, build)
}

// serveFiltered is serveCached with a per-request narrowing applied to whatever
// the shared entry holds.
func (s *Server) serveFiltered(w http.ResponseWriter, r *http.Request, path string, params url.Values, filter recordFilter, build func(context.Context) (cachedResponse, error)) {
	cache, enabled := s.cacheFor(path, params)
	if !enabled {
		resp, err := build(r.Context())
		if err != nil {
			s.writeUpstreamError(w, err)
			return
		}
		writeCached(w, filter.apply(resp))
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
		s.writeStored(w, ent, CacheFresh, filter)
		return
	case status == CacheStale:
		stale = &ent
	}

	// The flight is shared, so it runs on its own context rather than the leading
	// request's: one client disconnecting must not cancel the fan-out its
	// followers are waiting on, and the flight ends as soon as the last of them
	// goes. cfg.Timeout keeps it bounded.
	resp, err, _ := s.flights.Do(r.Context(), key, s.flightTimeout(), func(ctx context.Context) (cachedResponse, error) {
		built, buildErr := build(ctx)
		if buildErr != nil {
			return cachedResponse{}, buildErr
		}
		body, marshalErr := json.Marshal(built)
		if marshalErr != nil {
			s.log.Printf("warning: encoding cache entry for %s failed: %v", key, marshalErr)
			return built, nil
		}
		// The build succeeded, so the entry is worth storing even if the last
		// participant has already left and cancelled ctx: warming the cache for
		// the next caller is the whole point. Same bound as the flight so an
		// out-of-process cache cannot hang the store forever.
		putCtx, cancelPut := context.WithTimeout(context.WithoutCancel(ctx), s.flightTimeout())
		defer cancelPut()
		if putErr := cache.Put(putCtx, key, body); putErr != nil {
			s.log.Printf("warning: cache store for %s failed: %v", key, putErr)
		}
		return built, nil
	})
	if err != nil {
		// This caller left the flight because its own client went away, so there
		// is nobody to write to.
		if errors.Is(err, errFlightAbandoned) {
			return
		}
		// A refused query is answered, not degraded, so stale records are no reply
		// to it: the client has to see why the query was rejected.
		if stale != nil && !errors.As(err, new(*upstreamError)) {
			s.stale.markStale(s.now())
			s.log.Printf("warning: serving stale %s from cache (stored %s): %v",
				path, stale.StoredAt.UTC().Format(time.RFC3339), err)
			s.writeStored(w, *stale, CacheStale, filter)
			return
		}
		s.writeUpstreamError(w, err)
		return
	}
	s.stale.markFresh()
	s.setCacheHeaders(w, CacheMiss, time.Time{})
	writeCached(w, filter.apply(resp))
}

// http.Client reads a zero Timeout as "no deadline", but it would expire a
// context immediately, so an unset value falls back to the default.
func (s *Server) flightTimeout() time.Duration {
	if s.cfg.Timeout > 0 {
		return s.cfg.Timeout
	}
	return defaultTimeout
}

func (s *Server) writeStored(w http.ResponseWriter, ent CacheEntry, status CacheStatus, filter recordFilter) {
	var resp cachedResponse
	if err := json.Unmarshal(ent.Body, &resp); err != nil {
		s.log.Printf("warning: unreadable cache entry: %v", err)
		http.Error(w, "unreadable cache entry", http.StatusBadGateway)
		return
	}
	s.setCacheHeaders(w, status, ent.StoredAt)
	writeCached(w, filter.apply(resp))
}

// setCacheHeaders labels a response from a cache-backed path: X-Cache is
// hit/stale/miss and Age is whole seconds since the served copy was stored (0
// for a response built by this request). It reads the same clock the cache
// stamps entries with, so the two never disagree.
func (s *Server) setCacheHeaders(w http.ResponseWriter, status CacheStatus, storedAt time.Time) {
	label := "miss"
	switch status {
	case CacheFresh:
		label = "hit"
	case CacheStale:
		label = "stale"
	}
	age := 0
	if !storedAt.IsZero() {
		if secs := int(s.now().Sub(storedAt).Seconds()); secs > 0 {
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
	if resp.Configured > 0 {
		w.Header().Set(backendsHeader, strconv.Itoa(resp.Backends)+"/"+strconv.Itoa(resp.Configured))
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
	return s.mergeFactsWith(s.newSourceInjector(r.URL.Query().Get("query"), true))
}

// The path segment of /facts/<name> is the same outer `name` constraint the
// query gate already rules injection out on, so nothing is synthesised here:
// the source fact's own path is diverted to serveSourceFact before this.
// Suppression of an upstream record of the owned name stays on.
func (s *Server) mergeFactsByNameResponse(r *http.Request) func([]backendResult) []json.RawMessage {
	inject := s.newSourceInjector(r.URL.Query().Get("query"), true)
	inject.disableInject()
	return s.mergeFactsWith(inject)
}

func (s *Server) mergeFactsWith(inject *sourceInjector) func([]backendResult) []json.RawMessage {
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

// Returns one result per queried backend, in config order. Backends the prober
// currently has down are skipped so a known-dead backend costs no timeout.
func (s *Server) fanOut(ctx context.Context, path string, params url.Values) []backendResult {
	return s.fanOutTo(ctx, s.liveBackends(), path, params)
}

// fanOutAll ignores health state and asks every configured backend.
func (s *Server) fanOutAll(ctx context.Context, path string, params url.Values) []backendResult {
	return s.fanOutTo(ctx, s.cfg.Backends, path, params)
}

// liveBackends drops the backends currently marked unhealthy, but falls open to
// the full list when that would leave none: a broken prober, a wrong health
// path or a partition seen only by the prober must never black-hole traffic.
func (s *Server) liveBackends() []Backend {
	if s.health == nil {
		return s.cfg.Backends
	}
	live := make([]Backend, 0, len(s.cfg.Backends))
	for _, b := range s.cfg.Backends {
		if s.health.healthy(b.Name) {
			live = append(live, b)
		}
	}
	if len(live) == 0 {
		return s.cfg.Backends
	}
	return live
}

func (s *Server) fanOutTo(ctx context.Context, backends []Backend, path string, params url.Values) []backendResult {
	results := make([]backendResult, len(backends))
	var wg sync.WaitGroup
	for i, b := range backends {
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
		return nil, -1, newUpstreamError(resp.StatusCode, resp.Header.Get("Content-Type"), body)
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
	_, _ = w.Write(s.redactBackends(fallback.body))
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
	Status   string                   `json:"status"`
	Backends map[string]backendReport `json:"backends"`
	Query    queryReport              `json:"query"`
	Cache    cacheHealth              `json:"cache"`
}

// backendReport pairs this request's own reachability check with the background
// prober's running state for the same backend. The two answer different
// questions and must be read together: probe_unsupported means "not being
// verified", not "well", so reachable is the field that says whether the backend
// is actually answering queries right now.
type backendReport struct {
	// Reachable is this request's own live query to the backend, run against
	// every configured backend regardless of probe state: "ok" or the error text.
	Reachable string `json:"reachable"`
	// State is the background prober's verdict. probe_unsupported means the probe
	// has never answered on this backend, so its health is unknown — read Reachable
	// to find out whether it is up.
	State     string `json:"state"` // healthy | unhealthy | probe_unsupported | unprobed | unmonitored
	Failures  int    `json:"consecutive_failures"`
	Successes int    `json:"consecutive_successes"`
	LastProbe string `json:"last_probe,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// queryReport describes the most recent merged fan-out.
type queryReport struct {
	Partial       bool   `json:"partial"`
	Contributed   int    `json:"contributed"`
	Configured    int    `json:"configured"`
	PartialRounds uint64 `json:"partial_rounds"`
	LastPartial   string `json:"last_partial,omitempty"`
}

func (s *Server) queryHealth() queryReport {
	seen, contributed, configured, rounds, last := s.partial.snapshot()
	q := queryReport{Configured: len(s.cfg.Backends), PartialRounds: rounds}
	if seen {
		q.Contributed, q.Configured = contributed, configured
		q.Partial = contributed < configured
	}
	if !last.IsZero() {
		q.LastPartial = last.UTC().Format(time.RFC3339)
	}
	return q
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
	// Every backend is checked, including ones the prober has down, so the
	// report never hides a backend queries are currently skipping.
	results := s.fanOutAll(r.Context(), nodesPath, queryParams(probe))
	states := s.health.snapshot()

	report := healthReport{
		Backends: map[string]backendReport{},
		Query:    s.queryHealth(),
		Cache:    s.cacheHealth(),
	}
	healthy := 0
	for _, res := range results {
		b := backendReport{Reachable: "ok", State: stateUnmonitored}
		if res.err != nil {
			b.Reachable = res.err.Error()
		} else {
			healthy++
		}
		if st, ok := states[res.name]; ok {
			b.State = st.stateName()
			b.Failures, b.Successes = st.Failures, st.Successes
			b.LastError = st.LastErr
			if !st.LastProbe.IsZero() {
				b.LastProbe = st.LastProbe.UTC().Format(time.RFC3339)
			}
		}
		report.Backends[res.name] = b
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
