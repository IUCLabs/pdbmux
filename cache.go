package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net/url"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

// CacheStatus distinguishes the three outcomes of a lookup: nothing stored, a
// stored entry within its TTL, and a stored entry past it.
type CacheStatus int

const (
	CacheMiss CacheStatus = iota
	CacheFresh
	CacheStale
)

func (s CacheStatus) String() string {
	switch s {
	case CacheFresh:
		return "fresh"
	case CacheStale:
		return "stale"
	default:
		return "miss"
	}
}

// CacheEntry is a stored response body and the time it was stored.
type CacheEntry struct {
	Body     []byte
	StoredAt time.Time
}

// CacheStats is the cache state reported by /healthz.
type CacheStats struct {
	Backend      string `json:"backend"`
	Entries      int    `json:"entries"`
	StaleEntries int    `json:"stale_entries"`
	Bytes        int64  `json:"bytes"`
}

// Cache stores merged responses keyed by cacheKey. Get reports freshness rather
// than hiding expired entries so a caller can fall back to a stale body when the
// upstream fetch fails. The context and error exist for out-of-process backends
// (the reports cache lands on S3); an in-process backend ignores both.
//
// Put's context is detached from the request and flight that produced the body,
// so a store still runs when the last caller has walked away; it carries its own
// timeout.
//
// A Body handed back by Get aliases the cache's copy and must not be mutated.
type Cache interface {
	Get(ctx context.Context, key string) (CacheEntry, CacheStatus, error)
	Put(ctx context.Context, key string, body []byte) error
	Stats() CacheStats
}

// cacheKey is the request path when there are no params, else "<path>?<params>"
// where params is url.Values.Encode() over a copy whose repeated values have
// been sorted. Encode() already sorts keys ascending, so both the order params
// arrive in and the order of a repeated param's values are irrelevant to the
// key: one canonical string per distinct request.
func cacheKey(path string, params url.Values) string {
	if len(params) == 0 {
		return path
	}
	norm := make(url.Values, len(params))
	for k, vs := range params {
		sorted := append([]string(nil), vs...)
		sort.Strings(sorted)
		norm[k] = sorted
	}
	encoded := norm.Encode()
	if encoded == "" {
		return path
	}
	return path + "?" + encoded
}

// noopCache is the default for every path with no cache configured, so wiring a
// handler through the cache leaves its behaviour unchanged.
type noopCache struct{}

func (noopCache) Get(context.Context, string) (CacheEntry, CacheStatus, error) {
	return CacheEntry{}, CacheMiss, nil
}

func (noopCache) Put(context.Context, string, []byte) error { return nil }

func (noopCache) Stats() CacheStats { return CacheStats{Backend: "none"} }

type memoryEntry struct {
	key      string
	body     []byte
	storedAt time.Time
}

// memoryCache is a byte-bounded LRU. Expired entries are kept, not dropped, so
// they remain available as a stale fallback; only the byte budget evicts.
type memoryCache struct {
	ttl      time.Duration
	maxBytes int64
	now      func() time.Time

	mu    sync.Mutex
	ll    *list.List // front = most recently used
	items map[string]*list.Element
	bytes int64
}

func newMemoryCache(ttl time.Duration, maxBytes int64) *memoryCache {
	return &memoryCache{
		ttl:      ttl,
		maxBytes: maxBytes,
		now:      time.Now,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

func (c *memoryCache) Get(_ context.Context, key string) (CacheEntry, CacheStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return CacheEntry{}, CacheMiss, nil
	}
	c.ll.MoveToFront(el)
	e := el.Value.(*memoryEntry)
	status := CacheFresh
	if c.now().Sub(e.storedAt) >= c.ttl {
		status = CacheStale
	}
	return CacheEntry{Body: e.body, StoredAt: e.storedAt}, status, nil
}

func (c *memoryCache) Put(_ context.Context, key string, body []byte) error {
	// A response larger than the whole budget would evict everything else.
	if int64(len(body)) > c.maxBytes {
		return nil
	}
	stored := append([]byte(nil), body...)

	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		e := el.Value.(*memoryEntry)
		c.bytes += int64(len(stored)) - int64(len(e.body))
		e.body, e.storedAt = stored, c.now()
		c.ll.MoveToFront(el)
	} else {
		c.items[key] = c.ll.PushFront(&memoryEntry{key: key, body: stored, storedAt: c.now()})
		c.bytes += int64(len(stored))
	}
	for c.bytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		e := c.ll.Remove(back).(*memoryEntry)
		delete(c.items, e.key)
		c.bytes -= int64(len(e.body))
	}
	return nil
}

func (c *memoryCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := CacheStats{Backend: "memory", Entries: len(c.items), Bytes: c.bytes}
	now := c.now()
	for el := c.ll.Front(); el != nil; el = el.Next() {
		if now.Sub(el.Value.(*memoryEntry).storedAt) >= c.ttl {
			st.StaleEntries++
		}
	}
	return st
}

// flightGroup collapses concurrent identical builds so N simultaneous requests
// for one key cause one upstream fan-out.
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	resp cachedResponse
	err  error

	cancel context.CancelFunc
	// participants is the number of callers still waiting on this flight,
	// guarded by flightGroup.mu.
	participants int
}

// errFlightAbandoned reports that this caller stopped waiting because its own
// context ended. It says nothing about the flight, which may still be running
// for other participants.
var errFlightAbandoned = errors.New("abandoned the shared flight")

// flightPanic is a panic from a flight's fn, reported to the leader and to every
// waiter as an error so callers keep their error handling (stale fallback, 502)
// instead of seeing a zero-value success.
type flightPanic struct {
	value any
	stack []byte
}

func (p *flightPanic) Error() string {
	return fmt.Sprintf("panic building response: %v\n%s", p.value, p.stack)
}

// Do returns fn's result and whether this caller shared another's in-flight run.
//
// fn runs on a context of the flight's own: detached from every caller's,
// bounded by timeout, and cancelled once the last participant leaves. A caller
// that gives up returns errFlightAbandoned and leaves the flight running for
// whoever is still waiting, so one participant walking away can neither cancel
// nor fail the others, while a flight nobody waits on any more is dropped at
// once rather than holding upstream sockets until the timeout.
func (g *flightGroup) Do(ctx context.Context, key string, timeout time.Duration, fn func(context.Context) (cachedResponse, error)) (resp cachedResponse, err error, shared bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*flightCall)
	}
	c, shared := g.calls[key]
	if shared {
		c.participants++
		g.mu.Unlock()
	} else {
		flightCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		c = &flightCall{done: make(chan struct{}), cancel: cancel, participants: 1}
		g.calls[key] = c
		g.mu.Unlock()
		go g.run(flightCtx, key, c, fn)
	}
	defer g.leave(key, c)

	select {
	case <-c.done:
		return c.resp, c.err, shared
	case <-ctx.Done():
		return cachedResponse{}, fmt.Errorf("%w: %w", errFlightAbandoned, ctx.Err()), shared
	}
}

func (g *flightGroup) run(ctx context.Context, key string, c *flightCall, fn func(context.Context) (cachedResponse, error)) {
	defer func() {
		if r := recover(); r != nil {
			c.resp, c.err = cachedResponse{}, &flightPanic{value: r, stack: debug.Stack()}
		}
		// The key is released before the results are published, so a caller that
		// arrives late leads a new flight instead of joining a finished one.
		g.forget(key, c)
		close(c.done)
	}()

	c.resp, c.err = fn(ctx)
}

// leave drops one participant and, when it was the last, unregisters the key and
// cancels the flight so a lone requester disconnecting aborts the fan-out
// instead of pinning a socket per backend for the whole timeout. Unregistering
// under the same lock that admits joiners keeps anyone from joining a flight
// that is about to be cancelled.
func (g *flightGroup) leave(key string, c *flightCall) {
	g.mu.Lock()
	c.participants--
	last := c.participants == 0
	if last {
		g.unregister(key, c)
	}
	g.mu.Unlock()
	if last {
		c.cancel()
	}
}

func (g *flightGroup) forget(key string, c *flightCall) {
	g.mu.Lock()
	g.unregister(key, c)
	g.mu.Unlock()
}

func (g *flightGroup) unregister(key string, c *flightCall) {
	if cur, ok := g.calls[key]; ok && cur == c {
		delete(g.calls, key)
	}
}

// staleTracker records stale fallbacks for /healthz. serving flips back to false
// as soon as a response is served from a live fan-out or a fresh entry.
type staleTracker struct {
	mu      sync.Mutex
	serving bool
	served  uint64
	last    time.Time
}

func (t *staleTracker) markStale(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.serving = true
	t.served++
	t.last = now
}

func (t *staleTracker) markFresh() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.serving = false
}

func (t *staleTracker) snapshot() (serving bool, served uint64, last time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.serving, t.served, t.last
}
