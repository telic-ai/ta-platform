// Package logging provides the structured logger used across services.
package logging

import (
	"log/slog"
	"os"
)

// New returns a JSON structured logger writing to stdout.
func New() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler)
}
