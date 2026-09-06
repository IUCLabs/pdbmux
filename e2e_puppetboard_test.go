//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Puppetboard renders one environment at a time; "*" is its all-environments
// selector, percent-encoded because it is a path segment.
const pbAllEnvs = "/%2A"

// startHostTunnel publishes a host port into the harness network. Containers on
// that network reach it at testcontainers.HostInternal, which works where a
// bridge-gateway route does not: a host firewall commonly drops container-to-host
// traffic. The tunnel is only built once its own container is ready, so it has to
// be carried by a container that needs nothing from the host — Puppetboard exits
// at boot when PuppetDB is unreachable and could never bootstrap its own.
func startHostTunnel(ctx context.Context, t fatalf, netName string, port int) func() {
	t.Helper()
	image := imageFor("PDBMUX_E2E_TUNNEL_IMAGE", "docker.io/library/alpine:3")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:           image,
			Cmd:             []string{"sleep", "infinity"},
			Networks:        []string{netName},
			HostAccessPorts: []int{port},
			WaitingFor:      wait.ForExec([]string{"true"}),
		},
		Started: true,
	})
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		t.Fatalf("opening a host tunnel for port %d: %v", port, err)
	}
	return func() { _ = testcontainers.TerminateContainer(c) }
}

// startPuppetboard brings up one Puppetboard pointed at pdbmux through the host
// tunnel on the harness network.
func startPuppetboard(ctx context.Context, t fatalf, netName string, muxPort int) (string, func()) {
	t.Helper()
	image := imageFor("PDBMUX_E2E_PUPPETBOARD_IMAGE", "ghcr.io/voxpupuli/puppetboard:latest")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Networks:     []string{netName},
			ExposedPorts: []string{"80/tcp"},
			Env: map[string]string{
				"PUPPETDB_HOST":       testcontainers.HostInternal,
				"PUPPETDB_PORT":       fmt.Sprint(muxPort),
				"PUPPETDB_SSL_VERIFY": "False",
				// Puppetboard refuses to boot without one; the value is irrelevant here.
				"SECRET_KEY": "pdbmux-e2e",
			},
			WaitingFor: wait.ForHTTP("/").WithPort("80/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		t.Fatalf("starting Puppetboard against pdbmux: %v", err)
	}
	endpoint, err := c.PortEndpoint(ctx, "80/tcp", "http")
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		t.Fatalf("resolving the Puppetboard endpoint: %v", err)
	}
	return endpoint, func() { _ = testcontainers.TerminateContainer(c) }
}

func pbGet(t *testing.T, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(h.puppetboard + path)
	if err != nil {
		t.Fatalf("GET %s from Puppetboard: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s from Puppetboard: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

func pbPage(t *testing.T, path string) string {
	t.Helper()
	status, body := pbGet(t, path)
	if status != http.StatusOK {
		t.Fatalf("Puppetboard %s returned HTTP %d", path, status)
	}
	return body
}

// A real PuppetDB client has to see one estate, so the node list must name every
// node from both backends.
func TestPuppetboardNodeList(t *testing.T) {
	body := pbPage(t, pbAllEnvs+"/nodes")
	for _, cn := range allNodes {
		if !strings.Contains(body, cn) {
			t.Errorf("Puppetboard node list does not mention %s", cn)
		}
	}
	if strings.Contains(body, nodeGone) {
		t.Errorf("Puppetboard node list mentions the deactivated node %s", nodeGone)
	}
}

// The index reads a summed /nodes aggregate, so it renders the estate-wide count
// rather than one backend's.
func TestPuppetboardIndex(t *testing.T) {
	ctx := context.Background()
	body := pbPage(t, "/")

	const q = `["extract",[["function","count"]],["and",["=","catalog_environment","production"]]]`
	wantA := backendCount(ctx, t, h.a, nodesPath, q)
	wantB := backendCount(ctx, t, h.b, nodesPath, q)
	if want := fmt.Sprint(wantA + wantB); !strings.Contains(body, want) {
		t.Errorf("Puppetboard index does not show the estate-wide node count %s (backends report %d and %d)", want, wantA, wantB)
	}
}

// A node detail page resolves through the /nodes/<certname> path route, which is
// an unmerged pass-through: it has to find the node whichever backend holds it.
func TestPuppetboardNodeDetail(t *testing.T) {
	for _, cn := range allNodes {
		body := pbPage(t, pbAllEnvs+"/node/"+cn)
		if !strings.Contains(body, cn) {
			t.Errorf("Puppetboard node page for %s does not name it", cn)
		}
	}
}

// The facts overview lists fact names, which come from the unmerged /fact-names
// route, so only the first backend's names reach the page today.
func TestPuppetboardFactsOverview(t *testing.T) {
	body := pbPage(t, pbAllEnvs+"/facts")
	for _, name := range []string{"osfamily", "kernel", "only_a"} {
		if !strings.Contains(body, name) {
			t.Errorf("Puppetboard facts overview does not list %s", name)
		}
	}

	t.Run("names from the second backend are missing", func(t *testing.T) {
		// Known gap: /fact-names has no merge rule, so handleQuery falls through to
		// proxyUnmerged and the first backend to answer supplies the whole list.
		// Recorded rather than asserted; fails once the gap closes.
		if strings.Contains(body, "only_b") {
			t.Fatal("the facts overview now lists a fact name only the second backend holds: /fact-names is merged, so assert this properly and drop the skip")
		}
		t.Skip("known gap: /fact-names is an unmerged pass-through, so only_b and extra_b never reach the facts overview")
	})
}

// The single-fact drilldown is the page that exercises the /facts/<name> path
// route.
func TestPuppetboardFactDrilldown(t *testing.T) {
	if body := pbPage(t, pbAllEnvs+"/fact/osfamily"); !strings.Contains(body, "osfamily") {
		t.Errorf("Puppetboard fact page for osfamily does not name it")
	}

	// The page's table is filled from this endpoint, so it is what a user sees.
	var payload struct {
		Data [][]string `json:"data"`
	}
	body := pbPage(t, pbAllEnvs+"/fact/osfamily/json")
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decoding the fact drilldown table: %v: %s", err, body)
	}

	listed := map[string]bool{}
	for _, row := range payload.Data {
		for _, cn := range allNodes {
			if len(row) > 0 && strings.Contains(row[0], cn) {
				listed[cn] = true
			}
		}
	}

	// Known gap: Puppetboard fetches a single fact through
	// GET /pdb/query/v4/facts/<name>, a path route with no merge rule, so
	// proxyUnmerged streams back the first backend's answer alone. The other
	// backend's nodes go missing with no error. Tracked separately; this test
	// records the gap and fails once it closes.
	missing := []string{}
	for _, cn := range allNodes {
		if !listed[cn] {
			missing = append(missing, cn)
		}
	}
	if len(missing) == 0 {
		t.Fatal("the fact drilldown now lists every node: the /facts/<name> path route is merged, so assert this properly and drop the skip")
	}
	if !listed[nodeAlpha] {
		t.Fatalf("the fact drilldown lists neither backend's nodes (%v missing), which is not the pass-through behaviour under test", missing)
	}
	t.Skipf("known gap: /pdb/query/v4/facts/<name> is served unmerged, so the drilldown silently omits %v", missing)
}

// Reports are the least exercised merged path, and Puppetboard reads them
// through the same JSON table the report list renders from.
func TestPuppetboardReports(t *testing.T) {
	var payload struct {
		Data [][]string `json:"data"`
	}
	body := pbPage(t, pbAllEnvs+"/reports/json")
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decoding the reports table: %v: %s", err, body)
	}
	joined := strings.Join(flatten(payload.Data), " ")
	for _, cn := range allNodes {
		if !strings.Contains(joined, cn) {
			t.Errorf("Puppetboard report list does not mention %s", cn)
		}
	}
}

func flatten(rows [][]string) []string {
	var out []string
	for _, row := range rows {
		out = append(out, row...)
	}
	return out
}
