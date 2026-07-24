package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	appName        = "pdbmux"
	configFileName = "config.yaml"
	envPrefix      = "PDBMUX_"

	// defaultListen is the default HTTP listen address.
	defaultListen = ":8080"
	// defaultOldURL / defaultNewURL are the two PuppetDBs merged during the
	// VM -> k8s migration. old = legacy VM-registered puppetdbapi; new =
	// the k8s PuppetDB behind the gateway (TLS terminated there).
	defaultOldURL = "http://puppetdb.example.com:8080"
	defaultNewURL = "https://puppetdb.k8s.example.net"
	// defaultPrimary is the backend name used for pass-through (non-merged)
	// /pdb/query/v4/* paths and as static precedence for merge fallback.
	defaultPrimary = "new"

	defaultTimeout      = 10 * time.Second
	defaultFreshnessTTL = 30 * time.Second
)

// Backend is one upstream PuppetDB. URL is the base URL (scheme://host[:port]),
// without the /pdb/query/v4/... path — that is appended per request.
type Backend struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// Config holds every configurable value. Fields map 1:1 to config-file keys and
// env vars (PDBMUX_*). See Load for precedence.
type Config struct {
	// Listen is the HTTP listen address (host:port).
	Listen string `yaml:"listen"`
	// Backends is the ordered list of upstream PuppetDBs to fan out to.
	Backends []Backend `yaml:"backends"`
	// Primary is the backend Name used for transparent pass-through of
	// non-merged /pdb/query/v4/* paths.
	Primary string `yaml:"primary"`
	// Merge selects how /facts records are attributed to a backend when a
	// certname appears in both: "freshness" (query /nodes report_timestamp,
	// newer wins) or "static" (always prefer the Prefer backend).
	Merge string `yaml:"merge"`
	// Prefer names the backend that wins under static merge and as the
	// tie-breaker/fallback under freshness merge.
	Prefer string `yaml:"prefer"`
	// Timeout bounds each upstream request.
	Timeout time.Duration `yaml:"timeout"`
	// FreshnessTTL is how long a per-certname freshness map (from /nodes) is
	// cached under the "freshness" merge strategy.
	FreshnessTTL time.Duration `yaml:"freshness_ttl"`
}

const (
	mergeFreshness = "freshness"
	mergeStatic    = "static"
)

// DefaultConfig returns the built-in defaults: both migration PuppetDBs,
// freshness merge, "new" primary/preferred.
func DefaultConfig() Config {
	return Config{
		Listen: defaultListen,
		Backends: []Backend{
			{Name: "old", URL: defaultOldURL},
			{Name: "new", URL: defaultNewURL},
		},
		Primary:      defaultPrimary,
		Merge:        mergeFreshness,
		Prefer:       defaultPrimary,
		Timeout:      defaultTimeout,
		FreshnessTTL: defaultFreshnessTTL,
	}
}

// ConfigDir returns the XDG_CONFIG_HOME/pdbmux directory.
func ConfigDir() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, appName)
}

// ConfigPath returns the full path to the config file.
func ConfigPath() string {
	return filepath.Join(ConfigDir(), configFileName)
}

// Load reads the config file (if present), then applies env var overrides.
// Precedence (lowest -> highest): defaults < config file < env vars < flags
// (flags are applied by the caller). Backends can be overridden wholesale via
// PDBMUX_BACKENDS ("name=url,name=url").
func Load() (Config, error) {
	cfg := DefaultConfig()

	path := ConfigPath()
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return cfg, fmt.Errorf("reading config %s: %w", path, err)
	}
	if err == nil {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parsing config %s: %w", path, err)
		}
	}

	applyEnv(&cfg, os.Getenv)

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// applyEnv overlays PDBMUX_* env vars onto cfg. getenv is injected for testing.
func applyEnv(cfg *Config, getenv func(string) string) {
	if v := getenv(envPrefix + "LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := getenv(envPrefix + "PRIMARY"); v != "" {
		cfg.Primary = v
	}
	if v := getenv(envPrefix + "MERGE"); v != "" {
		cfg.Merge = v
	}
	if v := getenv(envPrefix + "PREFER"); v != "" {
		cfg.Prefer = v
	}
	if v := getenv(envPrefix + "TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Timeout = d
		}
	}
	if v := getenv(envPrefix + "FRESHNESS_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FreshnessTTL = d
		}
	}
	if v := getenv(envPrefix + "BACKENDS"); v != "" {
		if bs := parseBackends(v); len(bs) > 0 {
			cfg.Backends = bs
		}
	}
}

// parseBackends parses "name=url,name=url" into Backends. Entries without an
// "=" are skipped. Used for the PDBMUX_BACKENDS env override.
func parseBackends(s string) []Backend {
	var out []Backend
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, url, ok := strings.Cut(part, "=")
		name, url = strings.TrimSpace(name), strings.TrimSpace(url)
		if !ok || name == "" || url == "" {
			continue
		}
		out = append(out, Backend{Name: name, URL: url})
	}
	return out
}

// Validate checks the config is internally consistent and usable.
func (c Config) Validate() error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}
	seen := map[string]bool{}
	for _, b := range c.Backends {
		if b.Name == "" || b.URL == "" {
			return fmt.Errorf("backend requires both name and url: %+v", b)
		}
		if seen[b.Name] {
			return fmt.Errorf("duplicate backend name %q", b.Name)
		}
		seen[b.Name] = true
	}
	if !seen[c.Primary] {
		return fmt.Errorf("primary %q is not a configured backend", c.Primary)
	}
	switch c.Merge {
	case mergeFreshness, mergeStatic:
	default:
		return fmt.Errorf("merge must be %q or %q, got %q", mergeFreshness, mergeStatic, c.Merge)
	}
	if !seen[c.Prefer] {
		return fmt.Errorf("prefer %q is not a configured backend", c.Prefer)
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	return nil
}

// PrimaryBackend returns the backend named by Primary (guaranteed present after
// Validate).
func (c Config) PrimaryBackend() Backend {
	for _, b := range c.Backends {
		if b.Name == c.Primary {
			return b
		}
	}
	return c.Backends[0]
}

// writeDefaultConfig creates the config dir and writes a default config file.
func writeDefaultConfig() error {
	dir := ConfigDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}
	path := ConfigPath()
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config already exists at %s", path)
	}
	data, _ := yaml.Marshal(DefaultConfig())
	header := []byte("# pdbmux configuration\n" +
		"# A merging proxy over two PuppetDBs (old VM + new k8s) during migration.\n" +
		"# Env overrides: PDBMUX_LISTEN, PDBMUX_PRIMARY, PDBMUX_MERGE, PDBMUX_PREFER,\n" +
		"#   PDBMUX_TIMEOUT, PDBMUX_FRESHNESS_TTL, PDBMUX_BACKENDS (name=url,name=url).\n\n")
	if err := os.WriteFile(path, append(header, data...), 0o644); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}
	fmt.Println("Config written to", path)
	return nil
}

// durationString renders a duration for `config show` (falls back to a plain
// seconds count for zero to avoid "0s" ambiguity in logs).
func durationString(d time.Duration) string {
	if d == 0 {
		return "0"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}
