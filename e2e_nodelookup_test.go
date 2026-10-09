//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// nodeLookupBin locates the node-lookup CLI, which is an external client and so
// is not built by this repo. Set PDBMUX_E2E_NODE_LOOKUP to a binary or leave one
// on PATH.
func nodeLookupBin(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("PDBMUX_E2E_NODE_LOOKUP"); p != "" {
		return p
	}
	p, err := exec.LookPath("node-lookup")
	if err != nil {
		t.Skip("node-lookup is not on PATH; set PDBMUX_E2E_NODE_LOOKUP to its binary to run this test")
	}
	return p
}

func runNodeLookup(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, nodeLookupBin(t), args...)
	// node-lookup takes the full facts endpoint, not a base URL.
	cmd.Env = append(os.Environ(), "NODE_LOOKUP_URL="+h.mux.URL+factsPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node-lookup %v: %v: %s", args, err, out)
	}
	return string(out)
}

// node-lookup is a plain PuppetDB fact client, so pointing it at pdbmux has to
// yield the merged estate rather than one backend's nodes.
func TestNodeLookupSeesBothBackends(t *testing.T) {
	var got map[string]map[string]any
	out := runNodeLookup(t, "-j", "-F", "osfamily")
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decoding node-lookup JSON: %v: %s", err, out)
	}

	want := map[string]string{
		nodeAlpha:  "RedHat",
		nodeBeta:   "Debian",
		nodeGamma:  "Debian",
		nodeShared: "Debian", // the fresher backend's value, not backend A's RedHat
	}
	for cn, value := range want {
		facts, ok := got[cn]
		if !ok {
			t.Errorf("node-lookup did not return %s: %v", cn, out)
			continue
		}
		if facts["osfamily"] != value {
			t.Errorf("node-lookup osfamily for %s = %v, want %q", cn, facts["osfamily"], value)
		}
	}
	if _, ok := got[nodeGone]; ok {
		t.Errorf("node-lookup returned the deactivated node %s", nodeGone)
	}
}

// An all-facts lookup is a certname-constrained query, a shape pdbmux injects
// provenance into, so the CLI shows which backend answered alongside the real
// facts. A -F lookup names a fact and so is gated, which is why this uses -a.
func TestNodeLookupShowsProvenance(t *testing.T) {
	out := runNodeLookup(t, "-n", nodeShared, "-a")
	if !strings.Contains(out, defaultSourceFact) {
		t.Fatalf("node-lookup -a for %s did not list %s: %q", nodeShared, defaultSourceFact, out)
	}
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, defaultSourceFact) {
			continue
		}
		if !strings.Contains(line, backendBHost) {
			t.Fatalf("node-lookup reports %q for %s, want the owning backend %q", strings.TrimSpace(line), nodeShared, backendBHost)
		}
		return
	}
}
