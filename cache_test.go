package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// testFlightTimeout is long enough that a flight ending early can only be the
	// refcount, never the deadline.
	testFlightTimeout = 30 * time.Second

	// disconnectAfter is long enough for the fan-out to reach the backends and
	// short enough to leave the whole abort well inside cfg.Timeout.
	disconnectAfter = 25 * time.Millisecond
)

// fakeClock drives the cache's TTL without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// countingBackend is a PuppetDB stand-in that counts requests per path and can
// be made to fail or block, so cache hits and single-flight are observable.
type countingBackend struct {
	srv *httptest.Server

	mu     sync.Mutex
	hits   map[string]int
	bodies map[string]string
	fail   bool
	block  chan struct{}
}

func newCountingBackend(t *testing.T, bodies map[string]string) *countingBackend {
	t.Helper()
	cb := &countingBackend{hits: map[string]int{}, bodies: bodies}
	cb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cb.mu.Lock()
		cb.hits[r.URL.Path]++
		fail, block, body := cb.fail, cb.block, cb.bodies[r.URL.Path]
		cb.mu.Unlock()

		if block != nil {
			<-block
		}
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if body == "" {
			body = "[]"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(cb.srv.Close)
	return cb
}

func (cb *countingBackend) hitCount(path string) int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.hits[path]
}

// totalHits counts requests across every path, so a test can assert a route
// reached no backend at all.
func (cb *countingBackend) totalHits() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	n := 0
	for _, c := range cb.hits {
		n += c
	}
	return n
}

func (cb *countingBackend) setFail(v bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.fail = v
}

func (cb *countingBackend) setBody(path, body string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.bodies[path] = body
}

func (cb *countingBackend) setBlock(ch chan struct{}) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.block = ch
}

// newCachedServer builds a server whose facts cache runs on a fake clock.
func newCachedServer(t *testing.T, cfg Config) (*Server, *fakeClock) {
	t.Helper()
	srv := newTestServer(cfg)
	mc, ok := srv.factsCache.(*memoryCache)
	if !ok {
		t.Fatalf("expected a memory cache, got %T", srv.factsCache)
	}
	clk := newFakeClock()
	mc.now = clk.now
	srv.now = clk.now
	return srv, clk
}

func cacheTestConfig(aURL, bURL string) Config {
	cfg := testConfig(aURL, bURL, mergeStatic)
	cfg.FactsTTL = 30 * time.Second
	cfg.CacheBytes = defaultCacheSize
	return cfg
}

func TestCacheKey_CanonicalOrdering(t *testing.T) {
	a := cacheKey(factsPath, url.Values{
		"query":  {`["=","name","role"]`},
		"limit":  {"10"},
		"expand": {"z", "a"},
	})
	b := cacheKey(factsPath, url.Values{
		"expand": {"a", "z"},
		"limit":  {"10"},
		"query":  {`["=","name","role"]`},
	})
	if a != b {
		t.Errorf("param order must not change the key:\n %s\n %s", a, b)
	}
	if !strings.HasPrefix(a, factsPath+"?") {
		t.Errorf("key must start with the path, got %q", a)
	}
	if got := cacheKey(nodesPath, nil); got != nodesPath {
		t.Errorf("no params should give the bare path, got %q", got)
	}
	if cacheKey(factsPath, url.Values{"query": {"x"}}) == cacheKey(factsPath, url.Values{"query": {"y"}}) {
		t.Error("different queries must not share a key")
	}
	if cacheKey(factsPath, url.Values{"query": {"x"}}) == cacheKey(nodesPath, url.Values{"query": {"x"}}) {
		t.Error("different paths must not share a key")
	}
}

func TestMemoryCache_FreshThenStale(t *testing.T) {
	c := newMemoryCache(30*time.Second, 1<<20)
	clk := newFakeClock()
	c.now = clk.now

	if _, status, _ := c.Get(context.Background(), "k"); status != CacheMiss {
		t.Fatalf("empty cache should miss, got %s", status)
	}
	if err := c.Put(context.Background(), "k", []byte(`{"body":[]}`)); err != nil {
		t.Fatal(err)
	}

	ent, status, err := c.Get(context.Background(), "k")
	if err != nil || status != CacheFresh || string(ent.Body) != `{"body":[]}` {
		t.Fatalf("want a fresh hit, got %s %q (%v)", status, ent.Body, err)
	}

	clk.advance(29 * time.Second)
	if _, status, _ := c.Get(context.Background(), "k"); status != CacheFresh {
		t.Fatalf("inside the TTL should still be fresh, got %s", status)
	}

	clk.advance(2 * time.Second)
	ent, status, _ = c.Get(context.Background(), "k")
	if status != CacheStale {
		t.Fatalf("past the TTL should be stale, got %s", status)
	}
	if string(ent.Body) != `{"body":[]}` {
		t.Errorf("a stale entry must still carry its body, got %q", ent.Body)
	}
	if st := c.Stats(); st.Entries != 1 || st.StaleEntries != 1 {
		t.Errorf("stats should report 1 entry, 1 stale, got %+v", st)
	}
}

func TestMemoryCache_EvictsLeastRecentlyUsed(t *testing.T) {
	body := []byte("0123456789") // 10 bytes
	c := newMemoryCache(time.Minute, 25)
	ctx := context.Background()

	for _, k := range []string{"a", "b"} {
		if err := c.Put(ctx, k, body); err != nil {
			t.Fatal(err)
		}
	}
	// Reading "a" makes "b" the eviction candidate.
	if _, status, _ := c.Get(ctx, "a"); status != CacheFresh {
		t.Fatalf("a should be cached, got %s", status)
	}
	if err := c.Put(ctx, "c", body); err != nil {
		t.Fatal(err)
	}

	if _, status, _ := c.Get(ctx, "b"); status != CacheMiss {
		t.Errorf("b was least recently used and should have been evicted, got %s", status)
	}
	for _, k := range []string{"a", "c"} {
		if _, status, _ := c.Get(ctx, k); status != CacheFresh {
			t.Errorf("%s should have survived eviction, got %s", k, status)
		}
	}
	if st := c.Stats(); st.Entries != 2 || st.Bytes != 20 {
		t.Errorf("stats after eviction = %+v, want 2 entries / 20 bytes", st)
	}
}

func TestMemoryCache_SkipsOversizedEntry(t *testing.T) {
	c := newMemoryCache(time.Minute, 8)
	if err := c.Put(context.Background(), "big", []byte("123456789")); err != nil {
		t.Fatal(err)
	}
	if _, status, _ := c.Get(context.Background(), "big"); status != CacheMiss {
		t.Error("an entry larger than the whole budget must not be stored")
	}
	if st := c.Stats(); st.Bytes != 0 {
		t.Errorf("bytes = %d, want 0", st.Bytes)
	}
}

func TestNoopCache_AlwaysMisses(t *testing.T) {
	var c Cache = noopCache{}
	if err := c.Put(context.Background(), "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, status, _ := c.Get(context.Background(), "k"); status != CacheMiss {
		t.Errorf("noop cache must always miss, got %s", status)
	}
	if st := c.Stats(); st.Backend != "none" {
		t.Errorf("backend = %q, want none", st.Backend)
	}
}

func TestHandler_FactsCacheFreshHit(t *testing.T) {
	body := `[` + fact("h1", "role", "web", "") + `]`
	a := newCountingBackend(t, map[string]string{factsPath: body})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	first := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)
	second := doGet(t, srv.Handler(), factsPath, `["=","name","role"]`)

	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses %d/%d", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Errorf("cache hit changed the body:\n %s\n %s", first.Body.String(), second.Body.String())
	}
	if got := a.hitCount(factsPath); got != 1 {
		t.Errorf("backend a saw %d requests, want 1 (second served from cache)", got)
	}
	if got := b.hitCount(factsPath); got != 1 {
		t.Errorf("backend b saw %d requests, want 1", got)
	}

	// A different query is a different key and must go upstream.
	doGet(t, srv.Handler(), factsPath, `["=","name","osfamily"]`)
	if got := a.hitCount(factsPath); got != 2 {
		t.Errorf("a different query should refetch: %d requests, want 2", got)
	}
}

func TestHandler_FactsCacheExpires(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "role", "web", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	doGet(t, srv.Handler(), factsPath, "")
	clk.advance(31 * time.Second)
	doGet(t, srv.Handler(), factsPath, "")

	if got := a.hitCount(factsPath); got != 2 {
		t.Errorf("an expired entry should refetch: %d requests, want 2", got)
	}
}

func TestHandler_ServesStaleOnlyWhenBackendsFail(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "role", "old", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	warm := doGet(t, srv.Handler(), factsPath, "")
	if warm.Code != http.StatusOK {
		t.Fatalf("warm-up status %d", warm.Code)
	}
	if !strings.Contains(warm.Body.String(), `"old"`) {
		t.Fatalf("warm-up body = %s", warm.Body.String())
	}

	// Backends healthy but the entry expired: fresh data wins, never the stale copy.
	clk.advance(31 * time.Second)
	a.setBody(factsPath, `[`+fact("h1", "role", "new", "")+`]`)
	refetch := doGet(t, srv.Handler(), factsPath, "")
	if !strings.Contains(refetch.Body.String(), `"new"`) {
		t.Errorf("a healthy backend must not be shadowed by the stale entry: %s", refetch.Body.String())
	}
	if serving, _, _ := srv.stale.snapshot(); serving {
		t.Error("serving_stale must stay false while backends are healthy")
	}

	// Every backend down and the entry expired: the stale copy is served.
	clk.advance(31 * time.Second)
	a.setFail(true)
	b.setFail(true)
	stale := doGet(t, srv.Handler(), factsPath, "")
	if stale.Code != http.StatusOK {
		t.Fatalf("stale fallback status %d: %s", stale.Code, stale.Body.String())
	}
	if !strings.Contains(stale.Body.String(), `"new"`) {
		t.Errorf("stale body = %s, want the last cached copy", stale.Body.String())
	}

	serving, served, last := srv.stale.snapshot()
	if !serving || served != 1 || last.IsZero() {
		t.Errorf("stale tracker = %v/%d/%v, want serving=true served=1", serving, served, last)
	}
}

// The stored entry is the whole estate's record set and the pinned value narrows
// it per request, so the fallback has to keep narrowing: a client asking for one
// backend's records must not be handed every backend's because the entry expired.
func TestHandler_StaleSourceFactDrilldownStaysFilteredByOwner(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "osfamily", "RedHat", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h2", "osfamily", "Debian", "") + `]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	drilldown := sourceFactURL + "/a"
	want := map[string]string{"h1": "a"}

	warm := doGet(t, srv.Handler(), drilldown, "")
	if warm.Code != http.StatusOK {
		t.Fatalf("warm-up status %d: %s", warm.Code, warm.Body.String())
	}
	if got, n := sourceValues(t, warm.Body.Bytes(), defaultSourceFact); n != len(want) || !reflect.DeepEqual(got, want) {
		t.Fatalf("warm-up %s = %v (%d records), want %v", drilldown, got, n, want)
	}

	// Every backend down and the entry expired: the stale copy is served.
	clk.advance(31 * time.Second)
	a.setFail(true)
	b.setFail(true)

	rec := doGet(t, srv.Handler(), drilldown, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("stale fallback status %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(cacheStatusHeader); got != "stale" {
		t.Fatalf("%s = %q, want stale: the request did not take the fallback path", cacheStatusHeader, got)
	}
	got, n := sourceValues(t, rec.Body.Bytes(), defaultSourceFact)
	if n != len(want) || !reflect.DeepEqual(got, want) {
		t.Errorf("stale %s = %v (%d records), want only backend a's %v", drilldown, got, n, want)
	}
}

func TestHandler_NoCacheEntryMeansBackendFailureIs502(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	a.setFail(true)
	b.setFail(true)
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	rec := doGet(t, srv.Handler(), factsPath, "")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "all backends failed") {
		t.Errorf("body = %q", rec.Body.String())
	}
}

func TestHandler_SingleFlightCollapsesConcurrentRequests(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "role", "web", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	release := make(chan struct{})
	a.setBlock(release)
	b.setBlock(release)

	const callers = 16
	var wg sync.WaitGroup
	codes := make([]int, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = doGet(t, h, factsPath, "").Code
		}(i)
	}

	// The leader is parked inside the blocked backend, so every caller that
	// reaches the handler before the release joins its flight.
	waitFor(t, func() bool { return a.hitCount(factsPath) >= 1 })
	time.Sleep(250 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("caller %d got %d", i, code)
		}
	}
	if got := a.hitCount(factsPath); got != 1 {
		t.Errorf("backend a saw %d requests, want 1 for %d concurrent callers", got, callers)
	}
	if got := b.hitCount(factsPath); got != 1 {
		t.Errorf("backend b saw %d requests, want 1 for %d concurrent callers", got, callers)
	}
}

func TestFlightGroup_LeaderRunsOnce(t *testing.T) {
	var g flightGroup
	var calls, shared atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})

	run := func(context.Context) (cachedResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return cachedResponse{Body: json.RawMessage(`[]`), Records: -1}, nil
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, s := g.Do(context.Background(), "k", testFlightTimeout, run); s {
				shared.Add(1)
			}
		}()
	}
	<-entered
	time.Sleep(250 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("fn ran %d times, want 1", calls.Load())
	}
	if shared.Load() != 15 {
		t.Errorf("%d callers shared the flight, want 15", shared.Load())
	}
	// The key is released once the flight finishes.
	if _, _, s := g.Do(context.Background(), "k", testFlightTimeout, func(context.Context) (cachedResponse, error) {
		return cachedResponse{Records: -1}, nil
	}); s {
		t.Error("a later call must start its own flight")
	}
}

func TestFlightGroup_LeaderPanicFailsLeaderAndWaiters(t *testing.T) {
	var g flightGroup
	entered := make(chan struct{})
	release := make(chan struct{})

	type result struct {
		resp   cachedResponse
		err    error
		shared bool
	}
	waiters := make([]result, 8)
	var wg sync.WaitGroup

	var leader result
	wg.Add(1)
	go func() {
		defer wg.Done()
		leader.resp, leader.err, leader.shared = g.Do(context.Background(), "k", testFlightTimeout, func(context.Context) (cachedResponse, error) {
			close(entered)
			<-release
			panic("build exploded")
		})
	}()

	<-entered
	for i := range waiters {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			waiters[i].resp, waiters[i].err, waiters[i].shared = g.Do(context.Background(), "k", testFlightTimeout, func(context.Context) (cachedResponse, error) {
				t.Error("a waiter must not run its own fn")
				return cachedResponse{}, nil
			})
		}(i)
	}
	// Park the waiters in Wait() before the leader panics.
	time.Sleep(250 * time.Millisecond)
	close(release)
	wg.Wait()

	if leader.err == nil {
		t.Errorf("leader err = nil, want a panic error (resp %+v)", leader.resp)
	}
	for i, w := range waiters {
		if !w.shared {
			t.Errorf("waiter %d did not share the flight", i)
		}
		if w.err == nil {
			t.Fatalf("waiter %d unblocked with err = nil and resp %+v, want an error", i, w.resp)
		}
		if !strings.Contains(w.err.Error(), "build exploded") {
			t.Errorf("waiter %d err = %v, want the panic value", i, w.err)
		}
	}

	// The key is released on the panic path, so a later call leads its own flight.
	resp, err, shared := g.Do(context.Background(), "k", testFlightTimeout, func(context.Context) (cachedResponse, error) {
		return cachedResponse{Body: json.RawMessage(`[]`), Records: 0}, nil
	})
	if shared {
		t.Error("a call after a panicking flight must start its own flight")
	}
	if err != nil || resp.Records != 0 {
		t.Errorf("later call = (%+v, %v), want a clean success", resp, err)
	}
}

func TestServeCached_BuildPanicIs502ThenStale(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	panicBuild := func(context.Context) (cachedResponse, error) { panic("build exploded") }
	serve := func(build func(context.Context) (cachedResponse, error)) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, factsPath, nil)
		srv.serveCached(rec, req, factsPath, nil, build)
		return rec
	}

	// No entry to fall back to: a panicking build must not look like a success.
	rec := serve(panicBuild)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d (%s), want 502", rec.Code, rec.Body.String())
	}

	stored := `[` + fact("h1", "role", "web", "") + `]`
	rec = serve(func(context.Context) (cachedResponse, error) {
		return cachedResponse{Body: json.RawMessage(stored), Records: 1}, nil
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("priming status %d (%s)", rec.Code, rec.Body.String())
	}

	clk.advance(31 * time.Second)
	rec = serve(panicBuild)
	if rec.Code != http.StatusOK {
		t.Fatalf("stale fallback status %d (%s), want 200", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(rec.Body.String()); got != stored {
		t.Errorf("stale body = %s, want %s", got, stored)
	}
	if got := rec.Header().Get(recordsHeader); got != "1" {
		t.Errorf("%s = %q, want 1", recordsHeader, got)
	}
	if hr := health(t, srv); !hr.Cache.ServingStale || hr.Cache.StaleServed != 1 {
		t.Errorf("panic fallback not counted as stale: %+v", hr.Cache)
	}
}

func TestServeCached_BuildPanicFailsConcurrentWaiters(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	entered := make(chan struct{})
	release := make(chan struct{})
	serve := func(build func(context.Context) (cachedResponse, error)) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, factsPath, nil)
		srv.serveCached(rec, req, factsPath, nil, build)
		return rec
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var leaderRec *httptest.ResponseRecorder
	go func() {
		defer wg.Done()
		leaderRec = serve(func(context.Context) (cachedResponse, error) {
			close(entered)
			<-release
			panic("build exploded")
		})
	}()

	<-entered
	const waiters = 8
	recs := make([]*httptest.ResponseRecorder, waiters)
	for i := range waiters {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i] = serve(func(context.Context) (cachedResponse, error) {
				t.Error("a waiter must not run its own build")
				return cachedResponse{}, nil
			})
		}(i)
	}
	time.Sleep(250 * time.Millisecond)
	close(release)
	wg.Wait()

	if leaderRec.Code != http.StatusBadGateway {
		t.Errorf("leader status %d, want 502", leaderRec.Code)
	}
	for i, rec := range recs {
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("waiter %d got %d (%s), want 502 rather than an empty success",
				i, rec.Code, rec.Body.String())
		}
	}
}

// The leader's client disconnecting mid-fetch must not fail the followers whose
// own connections are healthy.
func TestHandler_LeaderDisconnectDoesNotFailFollowers(t *testing.T) {
	body := `[` + node("h1", "2026-01-01T00:00:00.000Z") + `]`
	a := newCountingBackend(t, map[string]string{nodesPath: body})
	b := newCountingBackend(t, map[string]string{nodesPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	release := make(chan struct{})
	a.setBlock(release)
	b.setBlock(release)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, nodesPath, nil).WithContext(leaderCtx)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()

	// Only once the leader is inside the fan-out does a second caller join its
	// flight rather than starting one of its own.
	waitFor(t, func() bool { return a.hitCount(nodesPath) >= 1 })

	follower := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(follower, httptest.NewRequest(http.MethodGet, nodesPath, nil))
	}()

	time.Sleep(250 * time.Millisecond)
	cancelLeader()
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if follower.Code != http.StatusOK {
		t.Fatalf("follower status %d (%s), want 200: a healthy client must not inherit the leader's cancellation",
			follower.Code, follower.Body.String())
	}
	// The leader built this body, so its provenance names the backend that
	// answered the leader's fan-out.
	want := `[` + stamped(t, node("h1", "2026-01-01T00:00:00.000Z"), defaultSourceFact, "a") + `]`
	if got := strings.TrimSpace(follower.Body.String()); !sameJSON(t, got, want) {
		t.Errorf("follower body = %s, want %s", got, want)
	}
	if got := a.hitCount(nodesPath); got != 1 {
		t.Errorf("backend a saw %d requests, want 1", got)
	}

	// The flight the leader started still populated the cache.
	warmed := doGet(t, h, nodesPath, "")
	if got := warmed.Header().Get(cacheStatusHeader); got != "hit" {
		t.Errorf("%s = %q, want hit: the abandoned leader's flight must still warm the cache", cacheStatusHeader, got)
	}
	if got := a.hitCount(nodesPath); got != 1 {
		t.Errorf("backend a saw %d requests after the cached read, want 1", got)
	}
	assertNoFlights(t, &srv.flights)
}

// blockingBackend parks every request until its own context ends, reporting when
// that happened, so an abandoned fan-out is observable from upstream.
func blockingBackend(t *testing.T, aborted chan<- time.Time) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			aborted <- time.Now()
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// A requester with nobody else on its flight must take the fan-out down with it
// rather than leave a socket per backend held open until cfg.Timeout.
func TestHandler_SoloDisconnectAbortsFanOutPromptly(t *testing.T) {
	aborted := make(chan time.Time, 4)
	cfg := cacheTestConfig(blockingBackend(t, aborted), blockingBackend(t, aborted))
	cfg.Timeout = 400 * time.Millisecond
	srv, _ := newCachedServer(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(disconnectAfter)
		cancel()
	}()

	start := time.Now()
	req := httptest.NewRequest(http.MethodGet, factsPath, nil).WithContext(ctx)
	srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
	if elapsed := time.Since(start); elapsed >= cfg.Timeout/2 {
		t.Errorf("handler returned after %s, want well under cfg.Timeout %s", elapsed, cfg.Timeout)
	}

	waitForReleases(t, aborted, len(cfg.Backends), start.Add(cfg.Timeout/2))
	assertNoFlights(t, &srv.flights)
}

// Distinct cache keys do not collapse into one flight, so disconnecting clients
// must not each hold len(backends) sockets for the whole timeout.
func TestHandler_DisconnectedRequestsDoNotPinBackends(t *testing.T) {
	aborted := make(chan time.Time, 128)
	cfg := cacheTestConfig(blockingBackend(t, aborted), blockingBackend(t, aborted))
	cfg.Timeout = 400 * time.Millisecond
	srv, _ := newCachedServer(t, cfg)
	h := srv.Handler()

	// Keep-alive plumbing outlives the requests, so the transport is ours to shut
	// down before counting goroutines.
	transport := &http.Transport{}
	srv.client.Transport = transport

	baseline := runtime.NumGoroutine()
	const callers = 25
	var wg sync.WaitGroup
	start := time.Now()
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				time.Sleep(disconnectAfter)
				cancel()
			}()
			target := factsPath + "?query=" + url.QueryEscape(strconv.Itoa(i))
			req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
			h.ServeHTTP(httptest.NewRecorder(), req)
		}(i)
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed >= cfg.Timeout/2 {
		t.Errorf("%d disconnecting callers took %s, want well under cfg.Timeout %s", callers, elapsed, cfg.Timeout)
	}

	// A caller cancelled before its fan-out was dispatched leaves the backend
	// nothing to release, so one release per caller is the floor.
	waitForReleases(t, aborted, callers, start.Add(cfg.Timeout/2))
	assertNoFlights(t, &srv.flights)
	transport.CloseIdleConnections()
	assertGoroutinesSettle(t, baseline, 2)
}

// waitForReleases fails unless at least want backend requests were released by
// cutoff, which is set well inside cfg.Timeout so only the flight going away can
// have freed them.
func waitForReleases(t *testing.T, aborted <-chan time.Time, want int, cutoff time.Time) {
	t.Helper()
	for got := 0; got < want; got++ {
		select {
		case <-aborted:
		case <-time.After(time.Until(cutoff)):
			t.Fatalf("%d of %d backend requests released before the cutoff, want all of them", got, want)
		}
	}
}

// A follower whose own client goes away must unpark rather than wait out the
// leader, and must not disturb the flight the others are sharing.
func TestFlightGroup_WaiterAbandonsOnContextCancel(t *testing.T) {
	var g flightGroup
	entered := make(chan struct{})
	release := make(chan struct{})

	var leaderResp cachedResponse
	var leaderErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		leaderResp, leaderErr, _ = g.Do(context.Background(), "k", testFlightTimeout, func(context.Context) (cachedResponse, error) {
			close(entered)
			<-release
			return cachedResponse{Body: json.RawMessage(`[1]`), Records: 1}, nil
		})
	}()
	<-entered

	// A patient waiter proves the flight survives the abandoning one.
	var patientResp cachedResponse
	var patientErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		patientResp, patientErr, _ = g.Do(context.Background(), "k", testFlightTimeout, waiterMustNotBuild(t))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var abandoned error
	var abandonedShared bool
	go func() {
		defer close(done)
		_, abandoned, abandonedShared = g.Do(ctx, "k", testFlightTimeout, waiterMustNotBuild(t))
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a waiter whose context was cancelled stayed parked on the leader")
	}
	if !errors.Is(abandoned, errFlightAbandoned) {
		t.Errorf("abandoning waiter err = %v, want errFlightAbandoned", abandoned)
	}
	if !errors.Is(abandoned, context.Canceled) {
		t.Errorf("abandoning waiter err = %v, want it to carry context.Canceled", abandoned)
	}
	if !abandonedShared {
		t.Error("the abandoning waiter did share the flight")
	}

	close(release)
	wg.Wait()

	if leaderErr != nil || leaderResp.Records != 1 {
		t.Errorf("leader = (%+v, %v), want a clean success", leaderResp, leaderErr)
	}
	if patientErr != nil || patientResp.Records != 1 {
		t.Errorf("patient waiter = (%+v, %v), want the leader's result", patientResp, patientErr)
	}

	g.mu.Lock()
	remaining := len(g.calls)
	g.mu.Unlock()
	if remaining != 0 {
		t.Errorf("%d flights left registered, want 0", remaining)
	}
}

// The last participant leaving must cancel the flight's context rather than let
// it burn the whole timeout, and must leave the group empty.
func TestFlightGroup_LastParticipantLeavingCancelsFlight(t *testing.T) {
	var g flightGroup
	entered := make(chan struct{})
	flightCancelled := make(chan struct{})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = g.Do(leaderCtx, "k", testFlightTimeout, func(ctx context.Context) (cachedResponse, error) {
			close(entered)
			<-ctx.Done()
			close(flightCancelled)
			return cachedResponse{}, ctx.Err()
		})
	}()
	<-entered

	const waiters = 8
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = g.Do(ctx, "k", testFlightTimeout, waiterMustNotBuild(t))
		}()
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	cancelLeader()
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiters stayed parked after their contexts were cancelled")
	}

	// testFlightTimeout is far longer, so only the refcount can have cancelled it.
	select {
	case <-flightCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the flight ran on after its last participant left")
	}
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the leader goroutine leaked")
	}

	assertNoFlights(t, &g)
}

// A leader walking away while a waiter is still parked must leave the flight's
// context untouched, so the waiter gets a real result.
func TestFlightGroup_LeaderLeavingKeepsFlightAliveForWaiter(t *testing.T) {
	var g flightGroup
	entered := make(chan struct{})
	release := make(chan struct{})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	var leaderErr error
	go func() {
		defer close(leaderDone)
		_, leaderErr, _ = g.Do(leaderCtx, "k", testFlightTimeout, func(ctx context.Context) (cachedResponse, error) {
			close(entered)
			<-release
			if err := ctx.Err(); err != nil {
				return cachedResponse{}, err
			}
			return cachedResponse{Body: json.RawMessage(`[1]`), Records: 1}, nil
		})
	}()
	<-entered

	waiterDone := make(chan struct{})
	var waiterResp cachedResponse
	var waiterErr error
	go func() {
		defer close(waiterDone)
		waiterResp, waiterErr, _ = g.Do(context.Background(), "k", testFlightTimeout, waiterMustNotBuild(t))
	}()

	time.Sleep(100 * time.Millisecond)
	cancelLeader()
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the leader stayed parked after its context was cancelled")
	}
	if !errors.Is(leaderErr, errFlightAbandoned) {
		t.Errorf("leader err = %v, want errFlightAbandoned", leaderErr)
	}

	close(release)
	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter never got a result")
	}
	if waiterErr != nil || waiterResp.Records != 1 {
		t.Errorf("waiter = (%+v, %v), want the flight's result", waiterResp, waiterErr)
	}

	assertNoFlights(t, &g)
}

// Random cancellations across many keys must leave nothing registered and no
// goroutines behind.
func TestFlightGroup_HammerRandomCancellations(t *testing.T) {
	var g flightGroup
	baseline := runtime.NumGoroutine()

	const callers = 400
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if i%3 == 0 {
				go func() {
					time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
					cancel()
				}()
			}
			key := strconv.Itoa(i % 17)
			_, _, _ = g.Do(ctx, key, testFlightTimeout, func(ctx context.Context) (cachedResponse, error) {
				select {
				case <-ctx.Done():
					return cachedResponse{}, ctx.Err()
				case <-time.After(time.Duration(rand.IntN(3000)) * time.Microsecond):
					return cachedResponse{Records: -1}, nil
				}
			})
		}(i)
	}
	wg.Wait()

	assertNoFlights(t, &g)
	assertGoroutinesSettle(t, baseline, 2)
}

func waiterMustNotBuild(t *testing.T) func(context.Context) (cachedResponse, error) {
	t.Helper()
	return func(context.Context) (cachedResponse, error) {
		t.Error("a waiter must not run its own fn")
		return cachedResponse{}, nil
	}
}

func assertNoFlights(t *testing.T, g *flightGroup) {
	t.Helper()
	g.mu.Lock()
	remaining := len(g.calls)
	g.mu.Unlock()
	if remaining != 0 {
		t.Errorf("%d flights left registered, want 0", remaining)
	}
}

// Goroutines unwind after their caller returns, so settling is polled rather
// than sampled once.
func assertGoroutinesSettle(t *testing.T, baseline, slack int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= baseline+slack {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("goroutines = %d, want back near the baseline of %d", got, baseline)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeCached_CacheStatusHeaders(t *testing.T) {
	stored := `[` + fact("h1", "role", "web", "") + `]`
	a := newCountingBackend(t, map[string]string{factsPath: stored})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	rec := doGet(t, h, factsPath, "")
	if got := rec.Header().Get(cacheStatusHeader); got != "miss" {
		t.Errorf("first request %s = %q, want miss", cacheStatusHeader, got)
	}
	if got := rec.Header().Get(ageHeader); got != "0" {
		t.Errorf("first request %s = %q, want 0", ageHeader, got)
	}

	clk.advance(7 * time.Second)
	rec = doGet(t, h, factsPath, "")
	if got := rec.Header().Get(cacheStatusHeader); got != "hit" {
		t.Errorf("cached request %s = %q, want hit", cacheStatusHeader, got)
	}
	if got := rec.Header().Get(ageHeader); got != "7" {
		t.Errorf("cached request %s = %q, want 7", ageHeader, got)
	}

	// Past the TTL with every backend down, the stale fallback must say so.
	clk.advance(24 * time.Second)
	a.setFail(true)
	b.setFail(true)
	rec = doGet(t, h, factsPath, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("stale fallback status %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(cacheStatusHeader); got != "stale" {
		t.Errorf("stale fallback %s = %q, want stale", cacheStatusHeader, got)
	}
	if got := rec.Header().Get(ageHeader); got != "31" {
		t.Errorf("stale fallback %s = %q, want 31 seconds since the entry was stored", ageHeader, got)
	}
	want := `[` + fact("h1", "role", "web", "") + `,` + factEnv("h1", defaultSourceFact, "a", "") + `]`
	if got := strings.TrimSpace(rec.Body.String()); !sameJSON(t, got, want) {
		t.Errorf("stale body = %s, want %s", got, want)
	}
}

// recordingCache captures what Put was handed, so a test can assert the store
// does not run on an already-cancelled context. memoryCache ignores its context
// and so cannot show the difference.
type recordingCache struct {
	putCalled chan struct{}
	putCtxErr error
	putBody   []byte
}

func newRecordingCache() *recordingCache {
	return &recordingCache{putCalled: make(chan struct{})}
}

func (c *recordingCache) Get(context.Context, string) (CacheEntry, CacheStatus, error) {
	return CacheEntry{}, CacheMiss, nil
}

// The fields are read only after putCalled closes, which orders the two.
func (c *recordingCache) Put(ctx context.Context, _ string, body []byte) error {
	c.putCtxErr = ctx.Err()
	c.putBody = append([]byte(nil), body...)
	close(c.putCalled)
	return nil
}

func (c *recordingCache) Stats() CacheStats { return CacheStats{Backend: "recording"} }

// A build that succeeded must still reach the cache once the last participant
// has left and cancelled the flight, or an out-of-process backend would drop the
// write and lose the entry the next caller would have hit warm.
func TestServeCached_PutRunsOnDetachedContext(t *testing.T) {
	srv := newTestServer(cacheTestConfig("http://backend.invalid", "http://backend.invalid"))
	cache := newRecordingCache()
	srv.factsCache = cache

	entered := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, factsPath, nil).WithContext(ctx)

	served := make(chan struct{})
	go func() {
		defer close(served)
		srv.serveCached(httptest.NewRecorder(), req, factsPath, nil, func(context.Context) (cachedResponse, error) {
			close(entered)
			<-release
			return cachedResponse{Body: json.RawMessage(`[]`), Records: -1}, nil
		})
	}()

	<-entered
	cancel()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("the abandoning request stayed parked")
	}

	// The flight's own context is cancelled by now, so only a detached one can
	// carry the store.
	close(release)
	select {
	case <-cache.putCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("a completed build never reached the cache")
	}
	if cache.putCtxErr != nil {
		t.Errorf("Put ran on a cancelled context: %v", cache.putCtxErr)
	}
	if len(cache.putBody) == 0 {
		t.Error("Put stored an empty body")
	}
}

func TestHandler_HealthzReportsCacheState(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "role", "web", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	hr := health(t, srv)
	if hr.Cache.Backend != "memory" || hr.Cache.TTL != "30s" {
		t.Fatalf("cache health = %+v, want memory/30s", hr.Cache)
	}
	if hr.Cache.Entries != 0 || hr.Cache.ServingStale {
		t.Fatalf("a cold cache should be empty and not stale: %+v", hr.Cache)
	}

	doGet(t, srv.Handler(), factsPath, "")
	hr = health(t, srv)
	if hr.Cache.Entries != 1 || hr.Cache.StaleEntries != 0 || hr.Cache.Bytes == 0 {
		t.Fatalf("after one request: %+v", hr.Cache)
	}

	clk.advance(31 * time.Second)
	if hr = health(t, srv); hr.Cache.StaleEntries != 1 {
		t.Fatalf("expired entry should count as stale: %+v", hr.Cache)
	}
	if hr.Cache.ServingStale {
		t.Error("an expired entry alone is not serving_stale")
	}

	a.setFail(true)
	b.setFail(true)
	doGet(t, srv.Handler(), factsPath, "")
	hr = health(t, srv)
	if !hr.Cache.ServingStale || hr.Cache.StaleServed != 1 || hr.Cache.LastStale == "" {
		t.Errorf("staleness not surfaced in /healthz: %+v", hr.Cache)
	}
	if hr.Status != "down" {
		t.Errorf("status = %q, want down", hr.Status)
	}
}

func TestHandler_HealthzReportsDisabledCache(t *testing.T) {
	a := newCountingBackend(t, nil)
	b := newCountingBackend(t, nil)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic)) // FactsTTL zero

	hr := health(t, srv)
	if hr.Cache.Backend != "none" || hr.Cache.TTL != "0" {
		t.Errorf("disabled cache health = %+v, want none/0", hr.Cache)
	}
}

func TestHandler_UncachedPathsStillRefetch(t *testing.T) {
	a := newCountingBackend(t, map[string]string{reportsPath: `[{"hash":"h"}]`})
	b := newCountingBackend(t, map[string]string{reportsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	doGet(t, srv.Handler(), reportsPath, "")
	doGet(t, srv.Handler(), reportsPath, "")
	if got := a.hitCount(reportsPath); got != 2 {
		t.Errorf("/reports is uncached: %d requests, want 2", got)
	}
}

func TestHandler_NodesRecordSetCached(t *testing.T) {
	a := newCountingBackend(t, map[string]string{nodesPath: `[{"certname":"h1"}]`})
	b := newCountingBackend(t, map[string]string{nodesPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	doGet(t, srv.Handler(), nodesPath, "")
	doGet(t, srv.Handler(), nodesPath, "")
	if got := a.hitCount(nodesPath); got != 1 {
		t.Errorf("merged /nodes is cached: %d requests, want 1", got)
	}
}

func TestHandler_NodesAggregateNotCached(t *testing.T) {
	a := newCountingBackend(t, map[string]string{nodesPath: `[{"count":90}]`})
	b := newCountingBackend(t, map[string]string{nodesPath: `[{"count":53}]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	first := doGet(t, srv.Handler(), nodesPath, nodeCountQuery)
	second := doGet(t, srv.Handler(), nodesPath, nodeCountQuery)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses %d/%d", first.Code, second.Code)
	}
	if got := a.hitCount(nodesPath); got != 2 {
		t.Errorf("/nodes aggregates are uncached: %d requests, want 2", got)
	}
	if got := counts(t, second.Body.Bytes(), "count"); !slices.Equal(got, []float64{143}) {
		t.Errorf("count = %v, want [143]", got)
	}
}

func TestHandler_FactsAggregateNotCached(t *testing.T) {
	const q = `["extract",[["function","count"]]]`
	a := newCountingBackend(t, map[string]string{factsPath: `[{"count":7}]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[{"count":10}]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	first := doGet(t, srv.Handler(), factsPath, q)
	second := doGet(t, srv.Handler(), factsPath, q)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses %d/%d", first.Code, second.Code)
	}
	if got := a.hitCount(factsPath); got != 2 {
		t.Errorf("/facts aggregates are uncached: %d requests, want 2", got)
	}
	if got := counts(t, second.Body.Bytes(), "count"); !slices.Equal(got, []float64{17}) {
		t.Errorf("count = %v, want [17]", got)
	}
}

func TestHandler_ResourcesAggregateNotCached(t *testing.T) {
	a := newCountingBackend(t, map[string]string{resourcesPath: `[{"count":7}]`})
	b := newCountingBackend(t, map[string]string{resourcesPath: `[{"count":5}]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	doGet(t, srv.Handler(), resourcesPath, nodeCountQuery)
	doGet(t, srv.Handler(), resourcesPath, nodeCountQuery)
	if got := a.hitCount(resourcesPath); got != 2 {
		t.Errorf("/resources aggregates are uncached: %d requests, want 2", got)
	}
}

func TestHandler_MetaAndMetricsNotCached(t *testing.T) {
	bodies := map[string]string{
		metaVersionPath:      `{"version":"8.4.0"}`,
		metaServerTimePath:   `{"server_time":"2026-01-01T00:00:00.000Z"}`,
		"/metrics/v2/read/x": `{"request":{},"value":{"Value":1},"status":200}`,
	}
	a := newCountingBackend(t, bodies)
	b := newCountingBackend(t, bodies)
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

	for path := range bodies {
		doGet(t, srv.Handler(), path, "")
		doGet(t, srv.Handler(), path, "")
		if got := a.hitCount(path); got != 2 {
			t.Errorf("%s is uncached: %d requests, want 2", path, got)
		}
	}
}

func TestConfig_FactsTTLClampedAtCap(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	clearEnv(t)

	path := filepath.Join(dir, appName, configFileName)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("facts_ttl: 5m\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FactsTTL != maxFactsTTL {
		t.Errorf("file facts_ttl = %s, want it clamped to %s", cfg.FactsTTL, maxFactsTTL)
	}

	t.Setenv(envPrefix+"FACTS_TTL", "10m")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FactsTTL != maxFactsTTL {
		t.Errorf("env facts_ttl = %s, want it clamped to %s", cfg.FactsTTL, maxFactsTTL)
	}
	if cfg.factsTTLClamped != 10*time.Minute {
		t.Errorf("pre-clamp value = %s, want 10m0s", cfg.factsTTLClamped)
	}

	out := captureStdout(t, func() { printConfig(cfg) })
	if !strings.Contains(out, "facts_ttl    : 30s (clamped from 600s, cap 30s)") {
		t.Errorf("config show must report the clamp, got:\n%s", out)
	}

	t.Setenv(envPrefix+"FACTS_TTL", "5s")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FactsTTL != 5*time.Second || cfg.factsTTLClamped != 0 {
		t.Errorf("a value under the cap must pass through: %s / %s", cfg.FactsTTL, cfg.factsTTLClamped)
	}

	t.Setenv(envPrefix+"FACTS_TTL", "0s")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.cacheEnabled() {
		t.Error("facts_ttl 0 must disable the cache")
	}
	out = captureStdout(t, func() { printConfig(cfg) })
	if !strings.Contains(out, "(cache disabled)") {
		t.Errorf("config show must say the cache is off, got:\n%s", out)
	}
}

func TestNewServer_ClampsFactsTTL(t *testing.T) {
	cfg := testConfig("http://a.invalid", "http://b.invalid", mergeStatic)
	cfg.FactsTTL = time.Hour
	cfg.CacheBytes = 1 << 20
	srv := newTestServer(cfg)

	mc, ok := srv.factsCache.(*memoryCache)
	if !ok {
		t.Fatalf("expected a memory cache, got %T", srv.factsCache)
	}
	if mc.ttl != maxFactsTTL {
		t.Errorf("cache ttl = %s, want %s", mc.ttl, maxFactsTTL)
	}
}

func TestConfig_ValidateRejectsNegativeCacheSettings(t *testing.T) {
	base := testConfig("http://a.invalid", "http://b.invalid", mergeStatic)
	for name, mutate := range map[string]func(*Config){
		"negative facts_ttl":         func(c *Config) { c.FactsTTL = -time.Second },
		"negative facts_cache_bytes": func(c *Config) { c.CacheBytes = -1 },
	} {
		cfg := base
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s should not validate", name)
		}
	}
}

func health(t *testing.T, srv *Server) healthReport {
	t.Helper()
	rec := doGet(t, srv.Handler(), "/healthz", "")
	var hr healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil {
		t.Fatalf("decode healthz: %v (%s)", err, rec.Body.String())
	}
	return hr
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

// sameJSON compares two JSON documents by value, so a test need not track the
// key order json.Marshal produces for a stamped record.
func sameJSON(t *testing.T, got, want string) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("unmarshal got %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want %s: %v", want, err)
	}
	return reflect.DeepEqual(g, w)
}

// stamped is a /nodes record with one extra string key, as the merge serves it.
func stamped(t *testing.T, raw, field, value string) string {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	obj[field] = json.RawMessage(strconv.Quote(value))
	out, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal stamped record: %v", err)
	}
	return string(out)
}

// The injector is per-request but a cache entry is shared, so a second caller is
// served a body built for the first. Provenance names the backend that supplied
// the data, which is a property of that fetch, so the shared body stays correct.
func TestHandler_CachedFactsKeepSourceAttribution(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "role", "web", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	first := doGet(t, h, factsPath, "")
	if got := first.Header().Get(cacheStatusHeader); got != "miss" {
		t.Fatalf("first %s = %q, want miss", cacheStatusHeader, got)
	}
	srcs, n := sourceValues(t, first.Body.Bytes(), defaultSourceFact)
	if n != 1 || srcs["h1"] != "a" {
		t.Fatalf("first request sources = %v (%d records), want h1 -> a", srcs, n)
	}

	second := doGet(t, h, factsPath, "")
	if got := second.Header().Get(cacheStatusHeader); got != "hit" {
		t.Fatalf("second %s = %q, want hit", cacheStatusHeader, got)
	}
	srcs, n = sourceValues(t, second.Body.Bytes(), defaultSourceFact)
	if n != 1 || srcs["h1"] != "a" {
		t.Errorf("cached request sources = %v (%d records), want exactly one h1 -> a", srcs, n)
	}
	if got := a.hitCount(factsPath); got != 1 {
		t.Errorf("backend a saw %d requests, want 1: the second read must come from the cache", got)
	}
}

// The same, for the /nodes stamp rather than the synthetic /facts record.
func TestHandler_CachedNodesKeepSourceStamp(t *testing.T) {
	a := newCountingBackend(t, map[string]string{nodesPath: `[` + node("h1", "2026-01-02T00:00:00.000Z") + `]`})
	b := newCountingBackend(t, map[string]string{nodesPath: `[` + node("h1", "2026-01-01T00:00:00.000Z") + `]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	first := doGet(t, h, nodesPath, "")
	if got := nodeSources(t, first.Body.Bytes(), defaultSourceFact); got["h1"] != "a" {
		t.Fatalf("first request stamp = %v, want h1 -> a", got)
	}

	second := doGet(t, h, nodesPath, "")
	if got := second.Header().Get(cacheStatusHeader); got != "hit" {
		t.Fatalf("second %s = %q, want hit", cacheStatusHeader, got)
	}
	want := `[` + stamped(t, node("h1", "2026-01-02T00:00:00.000Z"), defaultSourceFact, "a") + `]`
	if got := strings.TrimSpace(second.Body.String()); !sameJSON(t, got, want) {
		t.Errorf("cached body = %s, want %s", got, want)
	}
	if got := a.hitCount(nodesPath); got != 1 {
		t.Errorf("backend a saw %d requests, want 1", got)
	}
}

// Provenance is baked into the cached body, so it ages with the data it labels:
// while the entry is served the attribution is the one that fetch had, and the
// rebuild after the TTL picks up the move.
func TestHandler_CachedSourceAgesWithItsData(t *testing.T) {
	a := newCountingBackend(t, map[string]string{factsPath: `[` + fact("h1", "role", "web", "") + `]`})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, clk := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	doGet(t, h, factsPath, "")

	// The node moves to b while the entry is still fresh.
	a.setBody(factsPath, `[]`)
	b.setBody(factsPath, `[`+fact("h1", "role", "web", "")+`]`)

	cached := doGet(t, h, factsPath, "")
	if got := cached.Header().Get(cacheStatusHeader); got != "hit" {
		t.Fatalf("%s = %q, want hit", cacheStatusHeader, got)
	}
	if srcs, _ := sourceValues(t, cached.Body.Bytes(), defaultSourceFact); srcs["h1"] != "a" {
		t.Errorf("cached sources = %v, want h1 -> a: the body and its attribution come from the same fetch", srcs)
	}

	clk.advance(31 * time.Second)
	rebuilt := doGet(t, h, factsPath, "")
	if srcs, _ := sourceValues(t, rebuilt.Body.Bytes(), defaultSourceFact); srcs["h1"] != "b" {
		t.Errorf("rebuilt sources = %v, want h1 -> b once the entry expired", srcs)
	}
}

// The injection gate is a pure function of path and query, both of which are in
// the cache key, so a gated request can never be served an injected body cached
// for an ungated one.
func TestHandler_SourceGateIsPerCacheKey(t *testing.T) {
	body := `[` + fact("h1", "role", "web", "") + `]`
	a := newCountingBackend(t, map[string]string{factsPath: body})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	const nameFiltered = `["=","name","role"]`
	for _, pass := range []string{"first", "cached"} {
		open := doGet(t, h, factsPath, "")
		if srcs, n := sourceValues(t, open.Body.Bytes(), defaultSourceFact); n != 1 || srcs["h1"] != "a" {
			t.Errorf("%s unfiltered request sources = %v (%d records), want h1 -> a", pass, srcs, n)
		}
		gated := doGet(t, h, factsPath, nameFiltered)
		if _, n := sourceValues(t, gated.Body.Bytes(), defaultSourceFact); n != 0 {
			t.Errorf("%s name-filtered request carried %d synthetic record(s), want none", pass, n)
		}
		if got := strings.TrimSpace(gated.Body.String()); !sameJSON(t, got, body) {
			t.Errorf("%s name-filtered body = %s, want %s", pass, got, body)
		}
	}
	if got := a.hitCount(factsPath); got != 2 {
		t.Errorf("backend a saw %d requests, want 2: one per distinct cache key", got)
	}
}

// Suppression of an upstream fact of the configured name is part of the merged
// body, so it survives into the cache rather than being re-applied per request.
func TestHandler_SuppressionSurvivesCacheHit(t *testing.T) {
	upstream := `[` + fact("h1", defaultSourceFact, "somewhere-else", "") + `,` + fact("h1", "role", "web", "") + `]`
	a := newCountingBackend(t, map[string]string{factsPath: upstream})
	b := newCountingBackend(t, map[string]string{factsPath: `[]`})
	srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))
	h := srv.Handler()

	doGet(t, h, factsPath, "")
	cached := doGet(t, h, factsPath, "")
	if got := cached.Header().Get(cacheStatusHeader); got != "hit" {
		t.Fatalf("%s = %q, want hit", cacheStatusHeader, got)
	}
	srcs, n := sourceValues(t, cached.Body.Bytes(), defaultSourceFact)
	if n != 1 || srcs["h1"] != "a" {
		t.Errorf("cached sources = %v (%d records), want exactly one h1 -> a, the upstream value dropped", srcs, n)
	}
}
