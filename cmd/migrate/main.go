// Command migrate applies or rolls back the embedded Postgres migrations.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/telic-ai/ta-platform/internal/platform/config"
	storepostgres "github.com/telic-ai/ta-platform/internal/store/postgres"
	"github.com/telic-ai/ta-platform/internal/store/postgres/migrations"
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "down") {
		fmt.Fprintln(os.Stderr, "usage: migrate up|down")
		os.Exit(2)
	}
	cfg, err := config.Load("migrate")
	if err != nil {
		fatal(err)
	}
	client, err := storepostgres.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		fatal(err)
	}
	defer client.Close()
	runner := migrations.New(client.Pool())
	if os.Args[1] == "up" {
		err = runner.Up(context.Background())
	} else {
		err = runner.Down(context.Background())
	}
	if err != nil {
		fatal(err)
	}
	fmt.Printf("migration %s complete\n", os.Args[1])
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
