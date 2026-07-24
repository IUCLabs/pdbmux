package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	clearEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != defaultListen {
		t.Errorf("listen = %q, want %q", cfg.Listen, defaultListen)
	}
	if len(cfg.Backends) != 2 || cfg.Backends[0].Name != "old" || cfg.Backends[1].Name != "new" {
		t.Errorf("unexpected default backends: %+v", cfg.Backends)
	}
	if cfg.Merge != mergeFreshness || cfg.Primary != "new" {
		t.Errorf("unexpected defaults merge=%s primary=%s", cfg.Merge, cfg.Primary)
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
	body := "listen: :9999\nmerge: static\nprimary: old\nprefer: old\n"
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
	cfg := DefaultConfig()
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
			cfg := DefaultConfig()
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
	cfg := DefaultConfig()
	if cfg.PrimaryBackend().URL != defaultNewURL {
		t.Errorf("primary backend URL = %q, want %q", cfg.PrimaryBackend().URL, defaultNewURL)
	}
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"LISTEN", "PRIMARY", "MERGE", "PREFER", "TIMEOUT", "FRESHNESS_TTL", "BACKENDS"} {
		t.Setenv(envPrefix+k, "")
	}
}
