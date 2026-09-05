package main

import (
	"bytes"
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
	cfg, err := Load("")
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

	cfg, err := Load("")
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

	cfg, err := Load("")
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

const testConfigBody = "listen: \":9999\"\nmerge: static\nprimary: old\nprefer: old\n" +
	"backends:\n  - name: old\n    url: http://localhost:18080\n  - name: new\n    url: http://localhost:18081\n"

func writeConfigFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(testConfigBody), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoad_FileOnly(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	clearEnv(t)
	path := filepath.Join(dir, appName, configFileName)
	writeConfigFile(t, path)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != ":9999" || cfg.Merge != mergeStatic || len(cfg.Backends) != 2 {
		t.Errorf("file values not applied: %+v", cfg)
	}
	if cfg.SourcePath() != path {
		t.Errorf("source path = %q, want %q", cfg.SourcePath(), path)
	}
}

func TestLoad_EnvOnly_DefaultPathMissing(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	clearEnv(t)
	t.Setenv(envPrefix+"BACKENDS", "a=http://localhost:18080")
	t.Setenv(envPrefix+"LISTEN", "127.0.0.1:1234")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("a missing default config file must not be an error: %v", err)
	}
	if cfg.SourcePath() != "" {
		t.Errorf("no file was loaded, source path should be empty, got %q", cfg.SourcePath())
	}
	if cfg.Listen != "127.0.0.1:1234" || len(cfg.Backends) != 1 {
		t.Errorf("env values not applied: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("env-only config should validate: %v", err)
	}
}

// A mounted config file must load with neither HOME nor XDG_CONFIG_HOME set.
func TestLoad_ExplicitPath(t *testing.T) {
	mounted := filepath.Join(t.TempDir(), "mounted.yaml")
	writeConfigFile(t, mounted)

	for _, tc := range []struct {
		name string
		flag string
		env  string
	}{
		{"flag", mounted, ""},
		{"env", "", mounted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", "")
			t.Setenv("HOME", "")
			clearEnv(t)
			t.Setenv(envConfigPath, tc.env)

			cfg, err := Load(tc.flag)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if cfg.SourcePath() != mounted {
				t.Errorf("source path = %q, want %q", cfg.SourcePath(), mounted)
			}
			if cfg.Listen != ":9999" {
				t.Errorf("listen = %q, want :9999", cfg.Listen)
			}
		})
	}
}

func TestLoad_ExplicitPathMissingIsError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "typo.yaml")

	for _, tc := range []struct {
		name string
		flag string
		env  string
	}{
		{"flag", missing, ""},
		{"env", "", missing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", t.TempDir())
			clearEnv(t)
			t.Setenv(envConfigPath, tc.env)

			_, err := Load(tc.flag)
			if err == nil {
				t.Fatal("an explicitly named config file that does not exist must fail loudly")
			}
			if !strings.Contains(err.Error(), missing) {
				t.Errorf("error should name the missing path, got: %v", err)
			}
		})
	}
}

func TestLoad_ExplicitFileStillLosesToEnv(t *testing.T) {
	mounted := filepath.Join(t.TempDir(), "mounted.yaml")
	writeConfigFile(t, mounted)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	clearEnv(t)
	t.Setenv(envConfigPath, mounted)
	t.Setenv(envPrefix+"LISTEN", "127.0.0.1:1234")
	t.Setenv(envPrefix+"MERGE", mergeFreshness)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != "127.0.0.1:1234" || cfg.Merge != mergeFreshness {
		t.Errorf("env must beat the file: %+v", cfg)
	}
	if len(cfg.Backends) != 2 {
		t.Errorf("unset env must leave file backends alone: %+v", cfg.Backends)
	}
}

func TestResolveConfigPath_Precedence(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	clearEnv(t)
	defaultPath := filepath.Join(dir, appName, configFileName)

	if got, explicit := resolveConfigPath(""); got != defaultPath || explicit {
		t.Errorf("no file anywhere: got %q explicit=%v, want %q false", got, explicit, defaultPath)
	}

	writeConfigFile(t, defaultPath)
	if got, explicit := resolveConfigPath(""); got != defaultPath || explicit {
		t.Errorf("default search: got %q explicit=%v, want %q false", got, explicit, defaultPath)
	}

	t.Setenv(envConfigPath, "/from/env.yaml")
	if got, explicit := resolveConfigPath(""); got != "/from/env.yaml" || !explicit {
		t.Errorf("env should beat the search path: got %q explicit=%v", got, explicit)
	}
	if got, explicit := resolveConfigPath("/from/flag.yaml"); got != "/from/flag.yaml" || !explicit {
		t.Errorf("flag should beat env: got %q explicit=%v", got, explicit)
	}
}

func TestConfigSearchPaths_EndsAtSystemDir(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	paths := configSearchPaths()
	want := filepath.Join(systemConfigDir, configFileName)
	if len(paths) != 2 || paths[1] != want {
		t.Errorf("search paths = %v, want the system path %q last", paths, want)
	}

	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "")
	if paths := configSearchPaths(); len(paths) != 1 || paths[0] != want {
		t.Errorf("without HOME/XDG the search path should be just %q, got %v", want, paths)
	}
}

func TestPrintConfig_ReportsSource(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	clearEnv(t)

	out := captureStdout(t, func() {
		cfg, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		printConfig(cfg)
	})
	if !strings.Contains(out, "none loaded") || !strings.Contains(out, filepath.Join(dir, appName, configFileName)) {
		t.Errorf("config show should report nothing was loaded and what it searched, got:\n%s", out)
	}

	path := filepath.Join(dir, appName, configFileName)
	writeConfigFile(t, path)
	out = captureStdout(t, func() {
		cfg, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		printConfig(cfg)
	})
	if !strings.Contains(out, path+" (loaded)") {
		t.Errorf("config show should name the loaded file, got:\n%s", out)
	}
}

func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	f()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"CONFIG", "LISTEN", "PRIMARY", "MERGE", "PREFER", "TIMEOUT", "FRESHNESS_TTL", "BACKENDS"} {
		t.Setenv(envPrefix+k, "")
	}
}
