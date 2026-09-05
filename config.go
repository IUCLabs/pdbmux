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

	defaultListen  = ":8080"
	defaultOldURL  = "http://puppetdb.example.com:8080"
	defaultNewURL  = "https://puppetdb.k8s.example.net"
	defaultPrimary = "new"

	defaultTimeout      = 10 * time.Second
	defaultFreshnessTTL = 30 * time.Second
)

type Backend struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"` // base URL only; the query path is appended per request
}

type Config struct {
	Listen       string        `yaml:"listen"`
	Backends     []Backend     `yaml:"backends"`
	Primary      string        `yaml:"primary"`
	Merge        string        `yaml:"merge"`
	Prefer       string        `yaml:"prefer"` // wins under static merge, and breaks ties under freshness merge
	Timeout      time.Duration `yaml:"timeout"`
	FreshnessTTL time.Duration `yaml:"freshness_ttl"`
}

const (
	mergeFreshness = "freshness"
	mergeStatic    = "static"
)

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

func ConfigDir() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, appName)
}

func ConfigPath() string {
	return filepath.Join(ConfigDir(), configFileName)
}

// Precedence: defaults < config file < env vars < flags, and flags are applied by the caller.
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

// Parses the PDBMUX_BACKENDS form "name=url,name=url"; entries without an "=" are skipped.
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

func (c Config) PrimaryBackend() Backend {
	for _, b := range c.Backends {
		if b.Name == c.Primary {
			return b
		}
	}
	return c.Backends[0]
}

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

func durationString(d time.Duration) string {
	if d == 0 {
		return "0"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}
