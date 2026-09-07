package main

import (
	"net/http"
	"slices"
	"testing"
)

// aggregateProbes gives every entry in the production route table a request path
// to exercise. A route added to queryRoutes with no probe here fails
// TestQueryRoutes_EveryRouteIsProbed, so a new merged route cannot reach main
// without its aggregate behaviour being asserted.
var aggregateProbes = map[string]string{
	nodesPath:                     nodesPath,
	factsPath:                     factsPath,
	resourcesPath:                 resourcesPath,
	reportsPath:                   reportsPath,
	factsPath + "/<name>":         factsPath + "/os",
	factNamesPath:                 factNamesPath,
	eventsPath:                    eventsPath,
	eventCountsPath:               eventCountsPath,
	aggregateEventCountsPath:      aggregateEventCountsPath,
	reportsPath + "/<hash>/<sub>": reportsPath + "/abc123/events",
}

func TestQueryRoutes_EveryRouteIsProbed(t *testing.T) {
	for _, rt := range queryRoutes {
		probe, ok := aggregateProbes[rt.name]
		if !ok {
			t.Errorf("route %q has no aggregate probe; add one so its aggregate behaviour is asserted", rt.name)
			continue
		}
		if got := routeFor(probe).name; got != rt.name {
			t.Errorf("probe %q resolves to route %q, want %q", probe, got, rt.name)
		}
	}
}

// The guard is in handleQuery rather than in each handler, so this asserts it
// for every guarded route at once: a route added to the table inherits the
// assertion instead of needing its own test. Two identical requests also pin the
// cache bypass — an aggregate is a summed count, not the record set the cache
// stores, so it must reach the backends every time.
func TestQueryRoutes_GuardedRoutesSumAggregatesUncached(t *testing.T) {
	const q = `["extract",[["function","count"]],["=","environment","production"]]`
	for _, rt := range queryRoutes {
		if rt.unsummed != "" {
			continue
		}
		t.Run(rt.name, func(t *testing.T) {
			probe := aggregateProbes[rt.name]
			a := newCountingBackend(t, map[string]string{probe: `[{"count":90}]`})
			b := newCountingBackend(t, map[string]string{probe: `[{"count":53}]`})
			srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

			for i := range 2 {
				rec := doGet(t, srv.Handler(), probe, q)
				if rec.Code != http.StatusOK {
					t.Fatalf("request %d: status %d: %s", i, rec.Code, rec.Body.String())
				}
				if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{143}) {
					t.Fatalf("request %d: count = %v, want [143]; one backend's rows were kept instead of summed", i, got)
				}
			}
			if got := a.hitCount(probe); got != 2 {
				t.Errorf("backend asked %d times, want 2: the aggregate was cached", got)
			}
		})
	}
}

// The dispatch guard and the per-function combiners have to hold at once: every
// guarded route in the table folds each aggregate column by its own operation,
// so a max comes back as the larger of the backends' values rather than as the
// blanket sum a count gets. Asserting both columns of one row pins that the
// operation is chosen per column, not per request.
func TestQueryRoutes_GuardedRoutesCombinePerFunction(t *testing.T) {
	const q = `["extract",[["function","count"],["function","max","report_timestamp"]],["=","environment","production"]]`
	for _, rt := range queryRoutes {
		if rt.unsummed != "" {
			continue
		}
		t.Run(rt.name, func(t *testing.T) {
			probe := aggregateProbes[rt.name]
			a := newCountingBackend(t, map[string]string{probe: `[{"count":90,"max":90}]`})
			b := newCountingBackend(t, map[string]string{probe: `[{"count":53,"max":53}]`})
			srv, _ := newCachedServer(t, cacheTestConfig(a.srv.URL, b.srv.URL))

			rec := doGet(t, srv.Handler(), probe, q)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if got := counts(t, rec.Body.Bytes(), "count"); !slices.Equal(got, []float64{143}) {
				t.Errorf("count = %v, want [143]", got)
			}
			if got := counts(t, rec.Body.Bytes(), "max"); !slices.Equal(got, []float64{90}) {
				t.Errorf("max = %v, want [90]; the column was combined by the wrong operation", got)
			}
		})
	}
}

// The opt-out is deliberate, so widening it has to be deliberate too.
func TestQueryRoutes_UnsummedRoutesAreTheKnownOnes(t *testing.T) {
	want := []string{
		aggregateEventCountsPath,
		eventCountsPath,
		factNamesPath,
		reportsPath + "/<hash>/<sub>",
	}
	var got []string
	for _, rt := range queryRoutes {
		if rt.unsummed != "" {
			got = append(got, rt.name)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("routes opting out of the aggregate guard = %v, want %v", got, want)
	}
}
