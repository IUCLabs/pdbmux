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
	envConfigPath  = envPrefix + "CONFIG"

	// systemConfigDir is the last resort in the search order, and the path a
	// container mount (configmap, secret) is expected to land on.
	systemConfigDir = "/etc/" + appName

	defaultListen = ":8080"

	defaultTimeout      = 10 * time.Second
	defaultFreshnessTTL = 30 * time.Second

	defaultSourceFact = "pdbmux_source"

	// maxFactsTTL is a hard cap, not a default: a larger configured value is
	// clamped down to it rather than rejected, so a stray env var cannot make a
	// container crash-loop.
	maxFactsTTL      = 30 * time.Second
	defaultFactsTTL  = 30 * time.Second
	defaultCacheSize = int64(64 << 20)
)

var exampleBackends = []Backend{
	{Name: "pdb-a", URL: "http://puppetdb1.example.com:8080"},
	{Name: "pdb-b", URL: "http://puppetdb2.example.com:8080"},
}

type Backend struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"` // base URL only; the query path is appended per request
}

type Config struct {
	Listen       string        `yaml:"listen"`
	Backends     []Backend     `yaml:"backends"` // all equal; order is only a deterministic tie-break
	Merge        string        `yaml:"merge"`
	Timeout      time.Duration `yaml:"timeout"`
	FreshnessTTL time.Duration `yaml:"freshness_ttl"`
	FactsTTL     time.Duration `yaml:"facts_ttl"`         // 0 disables the /facts+/nodes cache
	CacheBytes   int64         `yaml:"facts_cache_bytes"` // byte budget for that cache

	SourceFact        string `yaml:"source_fact"`
	SourceFactEnabled bool   `yaml:"source_fact_enabled"`

	sourcePath      string        // file this config was read from, empty if none was found
	factsTTLClamped time.Duration // pre-clamp facts_ttl, zero when nothing was clamped
}

// SourcePath returns the config file Load read, or "" when none was loaded.
func (c Config) SourcePath() string { return c.sourcePath }

const (
	mergeFreshness = "freshness"
	mergeStatic    = "static"
)

func DefaultConfig() Config {
	return Config{
		Listen:            defaultListen,
		Merge:             mergeFreshness,
		Timeout:           defaultTimeout,
		FreshnessTTL:      defaultFreshnessTTL,
		FactsTTL:          defaultFactsTTL,
		CacheBytes:        defaultCacheSize,
		SourceFact:        defaultSourceFact,
		SourceFactEnabled: true,
	}
}

func ExampleConfig() Config {
	cfg := DefaultConfig()
	cfg.Backends = append([]Backend(nil), exampleBackends...)
	return cfg
}

func ConfigDir() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			return systemConfigDir
		}
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, appName)
}

func ConfigPath() string {
	return filepath.Join(ConfigDir(), configFileName)
}

// configSearchPaths lists the default config locations, highest priority first.
func configSearchPaths() []string {
	paths := []string{ConfigPath()}
	if system := filepath.Join(systemConfigDir, configFileName); system != paths[0] {
		paths = append(paths, system)
	}
	return paths
}

// explicitConfigPath returns the config path named by --config or PDBMUX_CONFIG,
// or "" when neither is set.
func explicitConfigPath(flagPath string) string {
	if flagPath != "" {
		return flagPath
	}
	return os.Getenv(envConfigPath)
}

// resolveConfigPath picks the config file to read: --config, else PDBMUX_CONFIG,
// else the first existing default search path. explicit reports whether the path
// was named outright, in which case a missing file is an error.
func resolveConfigPath(flagPath string) (path string, explicit bool) {
	if p := explicitConfigPath(flagPath); p != "" {
		return p, true
	}
	paths := configSearchPaths()
	for _, p := range paths {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, false
		}
	}
	return paths[0], false
}

// Precedence: defaults < config file < env vars < flags, and flags are applied by the caller.
func Load(flagPath string) (Config, error) {
	cfg := DefaultConfig()

	path, explicit := resolveConfigPath(flagPath)
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parsing config %s: %w", path, err)
		}
		cfg.sourcePath = path
	case os.IsNotExist(err) && !explicit:
		// No config file anywhere on the search path: defaults + env only.
	default:
		return cfg, fmt.Errorf("reading config %s: %w", path, err)
	}

	applyEnv(&cfg, os.Getenv)
	cfg.clampFactsTTL()
	return cfg, nil
}

// clampFactsTTL pins facts_ttl to maxFactsTTL, remembering the configured value
// so `config show` can say the cap was applied.
func (c *Config) clampFactsTTL() {
	if c.FactsTTL > maxFactsTTL {
		c.factsTTLClamped = c.FactsTTL
		c.FactsTTL = maxFactsTTL
	}
}

func applyEnv(cfg *Config, getenv func(string) string) {
	if v := getenv(envPrefix + "LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := getenv(envPrefix + "MERGE"); v != "" {
		cfg.Merge = v
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
	if v := getenv(envPrefix + "SOURCE_FACT"); v != "" {
		cfg.SourceFact = v
	}
	if v := getenv(envPrefix + "SOURCE_FACT_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.SourceFactEnabled = b
		}
	}
	if v := getenv(envPrefix + "FACTS_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FactsTTL = d
		}
	}
	if v := getenv(envPrefix + "FACTS_CACHE_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.CacheBytes = n
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

// configHint names the file a user should edit: the one actually loaded, else
// the default write target.
func (c Config) configHint() string {
	if c.sourcePath != "" {
		return c.sourcePath
	}
	return ConfigPath()
}

func (c Config) Validate() error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured: set %sBACKENDS to \"name=url,name=url\" or add a backends list to %s",
			envPrefix, c.configHint())
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
	switch c.Merge {
	case mergeFreshness, mergeStatic:
	default:
		return fmt.Errorf("merge must be %q or %q, got %q", mergeFreshness, mergeStatic, c.Merge)
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if c.SourceFactEnabled && c.SourceFact == "" {
		return fmt.Errorf("source_fact must be non-empty, or set source_fact_enabled to false")
	}
	if c.FactsTTL < 0 {
		return fmt.Errorf("facts_ttl must not be negative (0 disables the cache)")
	}
	if c.CacheBytes < 0 {
		return fmt.Errorf("facts_cache_bytes must not be negative")
	}
	return nil
}

// cacheEnabled reports whether a facts/nodes cache should be built: both a TTL
// and a byte budget are required.
func (c Config) cacheEnabled() bool { return c.FactsTTL > 0 && c.CacheBytes > 0 }

func writeDefaultConfig(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config already exists at %s", path)
	}
	data, _ := yaml.Marshal(ExampleConfig())
	header := []byte("# pdbmux configuration\n" +
		"# A merging proxy presenting one PuppetDB v4 query surface over several\n" +
		"# PuppetDB backends. The backend URLs below are placeholders — edit them.\n" +
		"# Env overrides: PDBMUX_LISTEN, PDBMUX_MERGE, PDBMUX_TIMEOUT,\n" +
		"#   PDBMUX_FRESHNESS_TTL, PDBMUX_FACTS_TTL, PDBMUX_FACTS_CACHE_BYTES,\n" +
		"#   PDBMUX_BACKENDS (name=url,name=url),\n" +
		"#   PDBMUX_SOURCE_FACT, PDBMUX_SOURCE_FACT_ENABLED.\n" +
		"# facts_ttl caches merged /facts and /nodes in memory; it is capped at 30s\n" +
		"# (a larger value is clamped) and 0 disables the cache.\n\n")
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
