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

// The facts overview lists fact names, which come from the merged /fact-names
// route, so both backends' names have to reach the page.
func TestPuppetboardFactsOverview(t *testing.T) {
	body := pbPage(t, pbAllEnvs+"/facts")
	// only_a lives in backend A alone; only_b and extra_b in backend B alone.
	for _, name := range []string{"osfamily", "kernel", "only_a", "only_b", "extra_b"} {
		if !strings.Contains(body, name) {
			t.Errorf("Puppetboard facts overview does not list %s", name)
		}
	}

	// A name the overview lists is a link a user can click, so the one pdbmux
	// owns has to lead to a page with every node on it rather than an empty one.
	t.Run("the owned fact is listed and its link resolves", func(t *testing.T) {
		if !strings.Contains(body, defaultSourceFact) {
			t.Fatalf("Puppetboard facts overview does not list %s", defaultSourceFact)
		}
		listed := pbFactRows(t, defaultSourceFact)
		for _, cn := range allNodes {
			if !listed[cn] {
				t.Errorf("the %s drilldown omits %s, so the overview links to a dead page", defaultSourceFact, cn)
			}
		}
	})
}

// pbFactRows reads the JSON table a Puppetboard fact page renders from, and
// returns the certnames it lists.
func pbFactRows(t *testing.T, name string) map[string]bool {
	t.Helper()
	var payload struct {
		Data [][]string `json:"data"`
	}
	body := pbPage(t, pbAllEnvs+"/fact/"+name+"/json")
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decoding the %s drilldown table: %v: %s", name, err, body)
	}
	listed := map[string]bool{}
	for _, row := range payload.Data {
		for _, cn := range allNodes {
			if len(row) > 0 && strings.Contains(row[0], cn) {
				listed[cn] = true
			}
		}
	}
	return listed
}

// The single-fact drilldown is the page that exercises the merged /facts/<name>
// path route, so every backend's nodes have to appear on it.
func TestPuppetboardFactDrilldown(t *testing.T) {
	if body := pbPage(t, pbAllEnvs+"/fact/osfamily"); !strings.Contains(body, "osfamily") {
		t.Errorf("Puppetboard fact page for osfamily does not name it")
	}

	// Puppetboard fetches a single fact through GET /pdb/query/v4/facts/<name>,
	// so the page is only whole if that path route merges every backend.
	listed := pbFactRows(t, "osfamily")
	missing := []string{}
	for _, cn := range allNodes {
		if !listed[cn] {
			missing = append(missing, cn)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the fact drilldown omits %v; /pdb/query/v4/facts/<name> must serve every backend's nodes", missing)
	}
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
