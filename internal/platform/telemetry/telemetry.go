// Package telemetry wires up process-wide tracing and metrics.
package telemetry

import "context"

// Shutdown stops any background telemetry exporters. It is safe to call
// even if Init was never called.
type Shutdown func(context.Context) error

// Init configures telemetry for serviceName. It currently returns a no-op
// exporter; wire an OTel SDK exporter here when the collector is available.
func Init(serviceName string) (Shutdown, error) {
	return func(context.Context) error { return nil }, nil
}
