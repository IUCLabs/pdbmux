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

	sourcePath string // file this config was read from, empty if none was found
}

// SourcePath returns the config file Load read, or "" when none was loaded.
func (c Config) SourcePath() string { return c.sourcePath }

const (
	mergeFreshness = "freshness"
	mergeStatic    = "static"
)

func DefaultConfig() Config {
	return Config{
		Listen:       defaultListen,
		Merge:        mergeFreshness,
		Timeout:      defaultTimeout,
		FreshnessTTL: defaultFreshnessTTL,
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
	return cfg, nil
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
	return nil
}

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
		"#   PDBMUX_FRESHNESS_TTL, PDBMUX_BACKENDS (name=url,name=url).\n\n")
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
