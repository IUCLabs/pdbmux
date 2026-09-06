//go:build e2e

package main

import (
	"context"
	"fmt"
	"time"
)

// Certnames the fixture places in each backend. shared lives in both and the two
// copies disagree, which is what the dedupe and freshness assertions rest on.
const (
	nodeAlpha  = "alpha.example.com"  // backend A only
	nodeBeta   = "beta.example.com"   // backend B only
	nodeGamma  = "gamma.example.com"  // backend B only
	nodeShared = "shared.example.com" // both backends
	nodeGone   = "gone.example.com"   // deactivated in backend A, must not surface
)

// Report end_time becomes a node's report_timestamp, which is the field the
// freshness merge compares, so shared's backend-B report is deliberately newer.
// The timestamps are relative to now because openvoxdb drops the resource events
// of a report that falls outside its retention window, which would leave the
// event assertions with nothing to see.
var (
	fixtureBase = time.Now().UTC().Truncate(time.Second)

	tsAlpha        = fixtureTime(-4 * time.Hour)
	tsBeta         = fixtureTime(-3 * time.Hour)
	tsGamma        = fixtureTime(-150 * time.Minute)
	tsSharedOnA    = fixtureTime(-4 * time.Hour)
	tsSharedOnB    = fixtureTime(-1 * time.Hour)
	tsGone         = fixtureTime(-4 * time.Hour)
	tsDeactivation = fixtureTime(0)
)

func fixtureTime(offset time.Duration) string {
	return fixtureBase.Add(offset).Format("2006-01-02T15:04:05.000Z")
}

const (
	backendAName = "pdb-a"
	backendBName = "pdb-b"
)

type nodeFixture struct {
	certname string
	facts    map[string]any
	// reportEnd is the report's end_time, and so the node's report_timestamp.
	reportEnd string
	// lines are the catalog File resources' line numbers, one resource each. They
	// give /resources a numeric column whose per-backend minimum, maximum, sum and
	// row count all differ, so no combiner can be mistaken for another.
	lines []int
}

// Backend A: 2 nodes, 7 facts. Backend B: 3 nodes, 10 facts. The counts are
// deliberately unequal so a summed aggregate cannot be mistaken for either
// backend's own number.
var fixtureA = []nodeFixture{
	{certname: nodeAlpha, reportEnd: tsAlpha, lines: []int{10, 12, 14}, facts: map[string]any{
		"osfamily": "RedHat", "kernel": "Linux", "role": "web", "only_a": "yes",
	}},
	{certname: nodeShared, reportEnd: tsSharedOnA, lines: []int{20}, facts: map[string]any{
		"osfamily": "RedHat", "kernel": "Linux", "owner": backendAName,
	}},
}

var fixtureB = []nodeFixture{
	{certname: nodeBeta, reportEnd: tsBeta, lines: []int{30}, facts: map[string]any{
		"osfamily": "Debian", "kernel": "Linux", "role": "db", "only_b": "yes", "extra_b": "1",
	}},
	{certname: nodeGamma, reportEnd: tsGamma, lines: []int{40}, facts: map[string]any{
		"osfamily": "Debian", "kernel": "Linux",
	}},
	{certname: nodeShared, reportEnd: tsSharedOnB, lines: []int{50}, facts: map[string]any{
		"osfamily": "Debian", "kernel": "Linux", "owner": backendBName,
	}},
}

func loadFixtures(ctx context.Context, t fatalf, a, b *backend) {
	t.Helper()
	for _, n := range fixtureA {
		loadNode(ctx, t, a, n)
	}
	for _, n := range fixtureB {
		loadNode(ctx, t, b, n)
	}
	// A deactivated node proves the merged view reflects each backend's own
	// filtering rather than a raw union of everything ever stored.
	loadNode(ctx, t, a, nodeFixture{certname: nodeGone, reportEnd: tsGone, lines: []int{100}, facts: map[string]any{"osfamily": "RedHat"}})
	a.submit(ctx, t, cmdDeactivateNode, verDeactivateNode, nodeGone, tsDeactivation, map[string]any{
		"certname":           nodeGone,
		"producer_timestamp": tsDeactivation,
	})

	a.waitQueueDrained(ctx, t)
	b.waitQueueDrained(ctx, t)
}

func loadNode(ctx context.Context, t fatalf, b *backend, n nodeFixture) {
	t.Helper()
	b.submit(ctx, t, cmdReplaceFacts, verReplaceFacts, n.certname, n.reportEnd, factsPayload(n))
	b.submit(ctx, t, cmdStoreReport, verStoreReport, n.certname, n.reportEnd, reportPayload(n))
	b.submit(ctx, t, cmdReplaceCatalog, verReplaceCatalog, n.certname, n.reportEnd, catalogPayload(n))
}

// catalogPayload is the "replace catalog" v9 wire format. Catalogs give the node
// a catalog_environment and populate /resources, which the aggregate assertions
// and Puppetboard's index both read.
func catalogPayload(n nodeFixture) map[string]any {
	resources := []any{
		map[string]any{
			"type": "Stage", "title": "main", "aliases": []string{}, "exported": false,
			"file": nil, "line": nil, "tags": []string{"stage"}, "parameters": map[string]any{},
		},
	}
	edges := []any{}
	for i, line := range n.lines {
		title := fileTitle(n.certname, i)
		resources = append(resources, map[string]any{
			"type": "File", "title": title, "aliases": []string{}, "exported": false,
			"file": "/etc/puppetlabs/code/site.pp", "line": line, "tags": []string{"file"},
			"parameters": map[string]any{"ensure": "present"},
		})
		edges = append(edges, map[string]any{
			"source":       map[string]any{"type": "Stage", "title": "main"},
			"target":       map[string]any{"type": "File", "title": title},
			"relationship": "contains",
		})
	}
	return map[string]any{
		"certname":           n.certname,
		"version":            "1",
		"environment":        "production",
		"transaction_uuid":   nil,
		"catalog_uuid":       nil,
		"code_id":            nil,
		"producer_timestamp": n.reportEnd,
		"producer":           "pdbmux-e2e",
		"edges":              edges,
		"resources":          resources,
	}
}

// The first File resource keeps the plain /tmp/<certname> title the report's
// event names; the rest sort after every other fixture title, so one backend
// holds the estate's largest resource title and the other its smallest.
func fileTitle(certname string, i int) string {
	if i == 0 {
		return "/tmp/" + certname
	}
	return fmt.Sprintf("/tmp/zz-%s-%d", certname, i)
}

// factsPayload is the "replace facts" v5 wire format: certname, environment,
// producer, producer_timestamp and the fact values.
func factsPayload(n nodeFixture) map[string]any {
	return map[string]any{
		"certname":           n.certname,
		"environment":        "production",
		"producer":           "pdbmux-e2e",
		"producer_timestamp": n.reportEnd,
		"values":             n.facts,
	}
}

// reportPayload is the "store report" v8 wire format. Several keys are required
// but nullable, and logs/metrics are flat arrays rather than the {data, href}
// envelope the query API returns them in.
func reportPayload(n nodeFixture) map[string]any {
	return map[string]any{
		"certname":              n.certname,
		"environment":           "production",
		"report_format":         12,
		"puppet_version":        "8.0.0",
		"configuration_version": "1",
		"transaction_uuid":      nil,
		"catalog_uuid":          nil,
		"code_id":               nil,
		"cached_catalog_status": "not_used",
		"start_time":            n.reportEnd,
		"end_time":              n.reportEnd,
		"producer_timestamp":    n.reportEnd,
		"producer":              "pdbmux-e2e",
		"noop":                  false,
		"noop_pending":          false,
		"corrective_change":     false,
		"status":                "changed",
		"metrics": []any{
			map[string]any{"category": "time", "name": "total", "value": 1.5},
		},
		"logs": []any{
			map[string]any{
				"level": "notice", "message": "e2e run for " + n.certname, "source": "Puppet",
				"tags": []string{"notice"}, "time": n.reportEnd, "file": nil, "line": nil,
			},
		},
		"resources": []any{
			map[string]any{
				"skipped": false, "timestamp": n.reportEnd,
				"resource_type": "File", "resource_title": "/tmp/" + n.certname,
				"file": "/etc/puppetlabs/code/site.pp", "line": 1,
				"containment_path": []string{"Stage[main]"}, "corrective_change": false,
				"events": []any{
					map[string]any{
						"status": "success", "timestamp": n.reportEnd, "property": "ensure",
						"new_value": "present", "old_value": "absent", "corrective_change": false,
						"message": eventMessage(n.certname),
					},
				},
			},
		},
	}
}

func eventMessage(certname string) string { return "e2e created /tmp/" + certname }
