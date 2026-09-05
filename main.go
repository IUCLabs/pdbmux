// Command pdbmux is a small merging HTTP proxy over several PuppetDB backends.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	var (
		cfg        Config
		configPath string
		listen     string
		merge      string
	)

	// Loaded lazily: --config is only known once cobra has parsed flags.
	loadConfig := func() error {
		c, err := Load(configPath)
		if err != nil {
			return err
		}
		cfg = c
		return nil
	}

	serve := func(cmd *cobra.Command) error {
		if err := loadConfig(); err != nil {
			return err
		}
		if cmd.Flags().Changed("listen") {
			cfg.Listen = listen
		}
		if cmd.Flags().Changed("merge") {
			cfg.Merge = merge
		}
		if err := cfg.Validate(); err != nil {
			return err
		}
		return runServer(cfg)
	}

	root := &cobra.Command{
		Use:   appName,
		Short: "Merging HTTP proxy over several PuppetDB backends.",
		Long: "pdbmux presents a single merged PuppetDB v4 query surface over several\n" +
			"PuppetDB backends, so clients see one consistent view of nodes, facts and\n" +
			"reports spanning all of them. Running pdbmux with no subcommand (or\n" +
			"`pdbmux serve`) starts the proxy.",
		SilenceUsage: true,
		RunE:         func(cmd *cobra.Command, args []string) error { return serve(cmd) },
	}

	pf := root.PersistentFlags()
	pf.StringVar(&configPath, "config", "", "Config file path (overrides PDBMUX_CONFIG and the default search path)")
	pf.StringVar(&listen, "listen", defaultListen, "HTTP listen address (overrides config and PDBMUX_LISTEN)")
	pf.StringVar(&merge, "merge", mergeFreshness, "Facts merge strategy: freshness or static")

	serveCmd := &cobra.Command{
		Use:          "serve",
		Short:        "Start the proxy (default action)",
		SilenceUsage: true,
		RunE:         func(cmd *cobra.Command, args []string) error { return serve(cmd) },
	}

	configCmd := &cobra.Command{Use: "config", Short: "Manage configuration"}
	configCmd.AddCommand(
		&cobra.Command{
			Use:          "init",
			Short:        "Write a default config file (--config path, else " + ConfigPath() + ")",
			SilenceUsage: true,
			RunE: func(cmd *cobra.Command, args []string) error {
				path := explicitConfigPath(configPath)
				if path == "" {
					path = ConfigPath()
				}
				return writeDefaultConfig(path)
			},
		},
		&cobra.Command{
			Use:          "show",
			Short:        "Print the active configuration",
			SilenceUsage: true,
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := loadConfig(); err != nil {
					return err
				}
				printConfig(cfg)
				return nil
			},
		},
	)

	versionCmd := &cobra.Command{
		Use:          "version",
		Short:        "Print the version",
		Run:          func(cmd *cobra.Command, args []string) { fmt.Println(version) },
		SilenceUsage: true,
	}

	root.AddCommand(serveCmd, configCmd, versionCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(cfg Config) error {
	logger := log.New(os.Stderr, "pdbmux: ", log.LstdFlags)
	srv := NewServer(cfg, logger)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Printf("listening on %s (merge=%s backends=%d)",
		cfg.Listen, cfg.Merge, len(cfg.Backends))

	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		logger.Println("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpSrv.Shutdown(ctx)
	}
}

func factsTTLString(cfg Config) string {
	s := durationString(cfg.FactsTTL)
	switch {
	case cfg.factsTTLClamped > 0:
		return fmt.Sprintf("%s (clamped from %s, cap %s)",
			s, durationString(cfg.factsTTLClamped), durationString(maxFactsTTL))
	case !cfg.cacheEnabled():
		return s + " (cache disabled)"
	}
	return s
}

func printConfig(cfg Config) {
	if p := cfg.SourcePath(); p != "" {
		fmt.Printf("config file  : %s (loaded)\n", p)
	} else {
		fmt.Printf("config file  : none loaded (searched %s)\n", strings.Join(configSearchPaths(), ", "))
	}
	fmt.Printf("listen       : %s\n", cfg.Listen)
	fmt.Printf("merge        : %s\n", cfg.Merge)
	fmt.Printf("timeout      : %s\n", durationString(cfg.Timeout))
	fmt.Printf("freshness_ttl: %s\n", durationString(cfg.FreshnessTTL))
	if cfg.SourceFactEnabled {
		fmt.Printf("source_fact  : %s\n", cfg.SourceFact)
	} else {
		fmt.Printf("source_fact  : disabled\n")
	}
	fmt.Printf("facts_ttl    : %s\n", factsTTLString(cfg))
	fmt.Printf("facts_cache  : %d bytes\n", cfg.CacheBytes)
	fmt.Println("backends:")
	for _, b := range cfg.Backends {
		fmt.Printf("  - %-8s %s\n", b.Name, b.URL)
	}
}
