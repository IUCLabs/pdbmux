package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testConfigValid returns a minimal valid config for Validate/merge tests, with
// neutral placeholder backends.
func testConfigValid() Config {
	cfg := DefaultConfig()
	cfg.Backends = []Backend{
		{Name: "old", URL: "http://localhost:18080"},
		{Name: "new", URL: "http://localhost:18081"},
	}
	cfg.Primary = "new"
	cfg.Prefer = "new"
	return cfg
}

func TestDefaultConfig_NoBackends(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Listen != defaultListen {
		t.Errorf("listen = %q, want %q", cfg.Listen, defaultListen)
	}
	if len(cfg.Backends) != 0 {
		t.Errorf("defaults must not ship backends, got %+v", cfg.Backends)
	}
	if cfg.Merge != mergeFreshness {
		t.Errorf("merge = %q, want %q", cfg.Merge, mergeFreshness)
	}
	if err := cfg.Validate(); err == nil {
		t.Error("defaults alone must not validate: backends are required")
	}
}

func TestLoad_NoBackendsLoadsButFailsValidation(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	clearEnv(t)

	// Load itself must succeed so `config init` / `version` work unconfigured.
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected a validation error when no backends are configured")
	}
	if !strings.Contains(err.Error(), envPrefix+"BACKENDS") {
		t.Errorf("error should name the env var to set, got: %v", err)
	}
}

func TestLoad_PrimaryDefaultsToFirstBackend(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	clearEnv(t)
	t.Setenv(envPrefix+"BACKENDS", "a=http://localhost:18080,b=http://localhost:18081")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Primary != "a" || cfg.Prefer != "a" {
		t.Errorf("primary/prefer should default to first backend, got %q/%q", cfg.Primary, cfg.Prefer)
	}
}

func TestLoad_FileAndEnvOverride(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	clearEnv(t)

	cfgDir := filepath.Join(dir, appName)
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "listen: :9999\nmerge: static\nprimary: old\nprefer: old\n" +
		"backends:\n  - name: old\n    url: http://localhost:18080\n  - name: new\n    url: http://localhost:18081\n"
	if err := os.WriteFile(filepath.Join(cfgDir, configFileName), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	// env beats file for listen.
	t.Setenv(envPrefix+"LISTEN", "127.0.0.1:1234")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != "127.0.0.1:1234" {
		t.Errorf("env should beat file for listen, got %q", cfg.Listen)
	}
	if cfg.Merge != mergeStatic || cfg.Primary != "old" {
		t.Errorf("file override failed: merge=%s primary=%s", cfg.Merge, cfg.Primary)
	}
}

func TestApplyEnv_Backends(t *testing.T) {
	cfg := testConfigValid()
	env := map[string]string{
		envPrefix + "BACKENDS":      "a=http://a:8080,b=http://b:8080",
		envPrefix + "PRIMARY":       "a",
		envPrefix + "PREFER":        "a",
		envPrefix + "TIMEOUT":       "3s",
		envPrefix + "FRESHNESS_TTL": "45s",
	}
	applyEnv(&cfg, func(k string) string { return env[k] })

	if len(cfg.Backends) != 2 || cfg.Backends[0].Name != "a" || cfg.Backends[1].URL != "http://b:8080" {
		t.Fatalf("PDBMUX_BACKENDS parse failed: %+v", cfg.Backends)
	}
	if cfg.Timeout != 3*time.Second || cfg.FreshnessTTL != 45*time.Second {
		t.Errorf("duration envs failed: %v %v", cfg.Timeout, cfg.FreshnessTTL)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"ok", func(*Config) {}, false},
		{"no backends", func(c *Config) { c.Backends = nil }, true},
		{"dup name", func(c *Config) { c.Backends = append(c.Backends, Backend{Name: "old", URL: "x"}) }, true},
		{"missing url", func(c *Config) { c.Backends[0].URL = "" }, true},
		{"primary not a backend", func(c *Config) { c.Primary = "ghost" }, true},
		{"prefer not a backend", func(c *Config) { c.Prefer = "ghost" }, true},
		{"bad merge", func(c *Config) { c.Merge = "wrong" }, true},
		{"zero timeout", func(c *Config) { c.Timeout = 0 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfigValid()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestParseBackends(t *testing.T) {
	got := parseBackends("a=http://a, b=http://b ,,broken,c=http://c")
	if len(got) != 3 {
		t.Fatalf("expected 3 valid backends, got %d: %+v", len(got), got)
	}
	if got[0].Name != "a" || got[2].URL != "http://c" {
		t.Errorf("unexpected parse: %+v", got)
	}
}

func TestPrimaryBackend(t *testing.T) {
	cfg := testConfigValid()
	if got, want := cfg.PrimaryBackend().URL, cfg.Backends[1].URL; got != want {
		t.Errorf("primary backend URL = %q, want %q", got, want)
	}
}

func TestExampleConfig_IsValidAndNeutral(t *testing.T) {
	cfg := ExampleConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("example config must validate: %v", err)
	}
	for _, b := range cfg.Backends {
		if !strings.Contains(b.URL, "example.com") {
			t.Errorf("example backend %q must use a placeholder host, got %q", b.Name, b.URL)
		}
	}
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"LISTEN", "PRIMARY", "MERGE", "PREFER", "TIMEOUT", "FRESHNESS_TTL", "BACKENDS"} {
		t.Setenv(envPrefix+k, "")
	}
}
