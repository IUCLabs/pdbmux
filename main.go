// Command pdbmux is a small merging HTTP proxy over several PuppetDB backends.
//
// When an estate runs more than one PuppetDB — during a migration, or across
// regions — a given node's current data lives in exactly one of them. pdbmux
// presents a single merged PuppetDB v4 query surface so clients see one
// consistent view:
//
//   - GET /pdb/query/v4/nodes  — fan out to all backends, dedupe by certname,
//     keep the record with the newer report_timestamp.
//   - GET /pdb/query/v4/facts  — fan out to all, and for a certname present in
//     more than one keep ALL facts from the backend holding that node's newer
//     report (freshness merge) or a static preferred backend (static merge).
//   - any other GET /pdb/query/v4/* — transparently proxied to the primary.
//   - GET /healthz — per-backend reachability.
//
// The query param is forwarded verbatim (PuppetDB AST JSON). If one backend
// errors/times out, the others' results are served; only if every backend fails
// does a merged endpoint return 502.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	cfg, err := Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}

	var (
		listen  string
		primary string
		merge   string
	)

	serve := func(cmd *cobra.Command) error {
		if cmd.Flags().Changed("listen") {
			cfg.Listen = listen
		}
		if cmd.Flags().Changed("primary") {
			cfg.Primary = primary
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
			"PuppetDB backends, so clients see one consistent view of nodes and facts\n" +
			"spanning all of them. Running pdbmux with no subcommand (or `pdbmux serve`)\n" +
			"starts the proxy.",
		SilenceUsage: true,
		RunE:         func(cmd *cobra.Command, args []string) error { return serve(cmd) },
	}

	pf := root.PersistentFlags()
	pf.StringVar(&listen, "listen", cfg.Listen, "HTTP listen address (overrides config and PDBMUX_LISTEN)")
	pf.StringVar(&primary, "primary", cfg.Primary, "Primary backend name for non-merged pass-through")
	pf.StringVar(&merge, "merge", cfg.Merge, "Facts merge strategy: freshness or static")

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
			Short:        "Write a default config file to " + ConfigPath(),
			SilenceUsage: true,
			RunE:         func(cmd *cobra.Command, args []string) error { return writeDefaultConfig() },
		},
		&cobra.Command{
			Use:          "show",
			Short:        "Print the active configuration",
			SilenceUsage: true,
			RunE: func(cmd *cobra.Command, args []string) error {
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

// runServer starts the HTTP server and blocks until SIGINT/SIGTERM, then
// gracefully shuts down.
func runServer(cfg Config) error {
	logger := log.New(os.Stderr, "pdbmux: ", log.LstdFlags)
	srv := NewServer(cfg, logger)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Printf("listening on %s (merge=%s primary=%s backends=%d)",
		cfg.Listen, cfg.Merge, cfg.Primary, len(cfg.Backends))

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

// printConfig renders the active config for `config show`.
func printConfig(cfg Config) {
	fmt.Printf("config file  : %s\n", ConfigPath())
	fmt.Printf("listen       : %s\n", cfg.Listen)
	fmt.Printf("primary      : %s\n", cfg.Primary)
	fmt.Printf("merge        : %s\n", cfg.Merge)
	fmt.Printf("prefer       : %s\n", cfg.Prefer)
	fmt.Printf("timeout      : %s\n", durationString(cfg.Timeout))
	fmt.Printf("freshness_ttl: %s\n", durationString(cfg.FreshnessTTL))
	fmt.Println("backends:")
	for _, b := range cfg.Backends {
		fmt.Printf("  - %-8s %s\n", b.Name, b.URL)
	}
}
