//go:build e2e

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcnet "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// openvoxdb refuses to start unless pg_trgm already exists in its database
	// (scf/migrate.clj require-extensions), so the harness creates it before boot.
	pgTrgmSQL = "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

	// Current wire versions accepted by openvoxdb 8 (command/constants.clj
	// supported-command-versions).
	cmdReplaceFacts   = "replace_facts"
	verReplaceFacts   = 5
	cmdStoreReport    = "store_report"
	verStoreReport    = 8
	cmdReplaceCatalog = "replace_catalog"
	verReplaceCatalog = 9
	cmdDeactivateNode = "deactivate_node"
	verDeactivateNode = 3

	// Blocking command submission: the POST returns only once the queued command
	// has been processed, so fixtures need no sleeps.
	commandWait = 90 * time.Second

	backendBoot = 5 * time.Minute
)

func imageFor(envVar, fallback string) string {
	if v := os.Getenv(envVar); v != "" {
		return v
	}
	return fallback
}

// backend is one PuppetDB stack: a PostgreSQL container and the openvoxdb
// container in front of it, reachable from the host on a fixed port so a
// stop/start cycle keeps the same URL.
type backend struct {
	name string
	url  string
	pg   testcontainers.Container
	pdb  testcontainers.Container
}

// reservePort picks a free host port and releases it, so the container can be
// published on a port that survives a restart.
func reservePort(t fatalf) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a host port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("releasing the reserved port: %v", err)
	}
	return port
}

func startBackend(ctx context.Context, t fatalf, name, netName string) *backend {
	t.Helper()

	pgAlias := name + "-pg"
	pgImage := imageFor("PDBMUX_E2E_POSTGRES_IMAGE", "docker.io/library/postgres:17-alpine")
	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    pgImage,
			Networks: []string{netName},
			NetworkAliases: map[string][]string{
				netName: {pgAlias},
			},
			Env: map[string]string{
				"POSTGRES_USER":     "openvoxdb",
				"POSTGRES_PASSWORD": "openvoxdb",
				"POSTGRES_DB":       "openvoxdb",
			},
			// Postgres restarts once during first-boot init, so the log line has to
			// be seen twice before the server is really accepting connections.
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("starting postgres for %s: %v", name, err)
	}

	// Nothing owns this postgres until the backend is fully built, so a failure
	// past this point has to take it down itself.
	fail := func(format string, args ...any) {
		_ = testcontainers.TerminateContainer(pg)
		t.Fatalf(format, args...)
	}

	code, out, err := pg.Exec(ctx, []string{"psql", "-U", "openvoxdb", "-d", "openvoxdb", "-c", pgTrgmSQL})
	if err != nil || code != 0 {
		body, _ := io.ReadAll(out)
		fail("creating pg_trgm for %s: code=%d err=%v out=%s", name, code, err, body)
	}

	port := reservePort(t)
	pdbImage := imageFor("PDBMUX_E2E_OPENVOXDB_IMAGE", "ghcr.io/openvoxproject/openvoxdb:8.15.0")
	pdb, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        pdbImage,
			Networks:     []string{netName},
			ExposedPorts: []string{"8080/tcp"},
			// A fixed host port, so the backend keeps its URL across the stop/start
			// the health test does.
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = mobynet.PortMap{
					mobynet.MustParsePort("8080/tcp"): []mobynet.PortBinding{
						{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)},
					},
				}
			},
			Env: map[string]string{
				// Without this the entrypoint waits for a puppetserver and switches
				// jetty to HTTPS; false leaves the default clear-text 8080 listener.
				"USE_OPENVOXSERVER":           "false",
				"OPENVOXDB_POSTGRES_HOSTNAME": pgAlias,
				"OPENVOXDB_POSTGRES_USER":     "openvoxdb",
				"OPENVOXDB_POSTGRES_PASSWORD": "openvoxdb",
				"OPENVOXDB_POSTGRES_DATABASE": "openvoxdb",
			},
			WaitingFor: waitForPuppetDBRunning(),
		},
		Started: true,
	})
	if err != nil {
		_ = testcontainers.TerminateContainer(pdb)
		fail("starting openvoxdb for %s: %v", name, err)
	}

	return &backend{name: name, url: fmt.Sprintf("http://127.0.0.1:%d", port), pg: pg, pdb: pdb}
}

// waitForPuppetDBRunning gates on the trapperkeeper status service reporting
// every service running, which is the same signal pdbmux's own prober reads.
func waitForPuppetDBRunning() wait.Strategy {
	return wait.ForHTTP("/status/v1/services").
		WithPort("8080/tcp").
		WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
		WithResponseMatcher(func(body io.Reader) bool {
			var services map[string]struct {
				State string `json:"state"`
			}
			if json.NewDecoder(body).Decode(&services) != nil || len(services) == 0 {
				return false
			}
			for _, svc := range services {
				if svc.State != "running" {
					return false
				}
			}
			return true
		}).
		WithStartupTimeout(backendBoot)
}

func (b *backend) terminate(ctx context.Context) {
	_ = testcontainers.TerminateContainer(b.pdb)
	_ = testcontainers.TerminateContainer(b.pg)
	_ = ctx
}

// stop kills the PuppetDB process so the port refuses connections, which is what
// pdbmux's prober and fan-out see when a backend dies.
func (b *backend) stop(ctx context.Context, t fatalf) {
	t.Helper()
	timeout := 30 * time.Second
	if err := b.pdb.Stop(ctx, &timeout); err != nil {
		t.Fatalf("stopping backend %s: %v", b.name, err)
	}
}

func (b *backend) start(ctx context.Context, t fatalf) {
	t.Helper()
	if err := b.pdb.Start(ctx); err != nil {
		t.Fatalf("starting backend %s: %v", b.name, err)
	}
	b.waitReady(ctx, t)
}

func (b *backend) waitReady(ctx context.Context, t fatalf) {
	t.Helper()
	deadline := time.Now().Add(backendBoot)
	for time.Now().Before(deadline) {
		if b.queueDepth(ctx) >= 0 {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("backend %s did not become ready within %s", b.name, backendBoot)
}

// queueDepth reads the command queue depth the status service publishes
// (status.clj's :queue_depth), or -1 when the backend is not answering or has
// not finished starting. Draining is a real signal, not a sleep.
func (b *backend) queueDepth(ctx context.Context) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url+"/status/v1/services", nil)
	if err != nil {
		return -1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return -1
	}
	var services struct {
		PuppetDB struct {
			State  string `json:"state"`
			Status struct {
				QueueDepth      *int `json:"queue_depth"`
				MaintenanceMode bool `json:"maintenance_mode?"`
			} `json:"status"`
		} `json:"puppetdb-status"`
	}
	if json.NewDecoder(resp.Body).Decode(&services) != nil {
		return -1
	}
	s := services.PuppetDB
	if s.State != "running" || s.Status.MaintenanceMode || s.Status.QueueDepth == nil {
		return -1
	}
	return *s.Status.QueueDepth
}

func (b *backend) waitQueueDrained(ctx context.Context, t fatalf) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if b.queueDepth(ctx) == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("backend %s command queue did not drain", b.name)
}

// commandResult is the blocking-submit reply: processed/timed_out say whether
// the command actually landed, and error carries a processing failure.
type commandResult struct {
	UUID      string `json:"uuid"`
	Processed bool   `json:"processed"`
	TimedOut  bool   `json:"timed_out"`
	Error     string `json:"error"`
}

// submit posts one command and blocks until openvoxdb has processed it, so the
// caller can query for its effect immediately afterwards.
func (b *backend) submit(ctx context.Context, t fatalf, command string, version int, certname, producerTimestamp string, payload any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encoding %s payload for %s: %v", command, certname, err)
	}
	params := url.Values{
		"certname":                   {certname},
		"command":                    {command},
		"version":                    {fmt.Sprint(version)},
		"producer-timestamp":         {producerTimestamp},
		"secondsToWaitForCompletion": {fmt.Sprint(int(commandWait.Seconds()))},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.url+"/pdb/cmd/v1?"+params.Encode(), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building %s request for %s: %v", command, certname, err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: commandWait + 30*time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("submitting %s for %s to %s: %v", command, certname, b.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)

	var res commandResult
	if json.Unmarshal(raw, &res) != nil {
		t.Fatalf("unreadable %s reply for %s from %s: HTTP %d %s", command, certname, b.name, resp.StatusCode, raw)
	}
	if !res.Processed || res.TimedOut || res.Error != "" {
		t.Fatalf("%s for %s on %s was not processed: HTTP %d processed=%v timed_out=%v error=%s",
			command, certname, b.name, resp.StatusCode, res.Processed, res.TimedOut, res.Error)
	}
}

// query runs a GET against this backend directly, bypassing pdbmux, so a test
// can compare the merged answer with the raw ones.
func (b *backend) query(ctx context.Context, t fatalf, path string, params url.Values) []map[string]any {
	t.Helper()
	target := b.url + path
	if len(params) > 0 {
		target += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("building query for %s: %v", b.name, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("querying %s%s: %v", b.name, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("querying %s%s: HTTP %d: %s", b.name, path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err != nil {
		t.Fatalf("decoding %s%s: %v: %s", b.name, path, err, body)
	}
	return rows
}

// queryStatus is query without the body, for asserting what a backend rejects.
func (b *backend) queryStatus(ctx context.Context, t fatalf, path string, params url.Values) int {
	t.Helper()
	target := b.url + path
	if len(params) > 0 {
		target += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("building query for %s: %v", b.name, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("querying %s%s: %v", b.name, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func newNetwork(ctx context.Context, t fatalf) (string, func()) {
	t.Helper()
	nw, err := tcnet.New(ctx)
	if err != nil {
		t.Fatalf("creating the harness network: %v", err)
	}
	return nw.Name, func() { _ = nw.Remove(ctx) }
}
