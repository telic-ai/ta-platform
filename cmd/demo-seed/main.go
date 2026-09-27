// Command demo-seed creates a demo company whose members have ready-made
// bearer sessions, and prints them as JSON. For local demos only.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/telic-ai/ta-platform/internal/demo"
	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/store/postgres"
)

func main() {
	cfg, err := config.Load("demo-seed")
	if err != nil {
		fatal(err)
	}
	if cfg.Env != "local" {
		fatal(fmt.Errorf("demo-seed only runs with ENV=local (ENV=%s)", cfg.Env))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		fatal(err)
	}
	defer client.Close()
	result, err := demo.Seed(ctx, client.Pool(), "Demo Co", 7*24*time.Hour)
	if err != nil {
		fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(result)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "demo-seed:", err)
	os.Exit(1)
}
