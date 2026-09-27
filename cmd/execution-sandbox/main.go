package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/telic-ai/ta-platform/internal/platform/config"
	"github.com/telic-ai/ta-platform/internal/platform/logging"
	"github.com/telic-ai/ta-platform/internal/sandbox"
	"github.com/telic-ai/ta-platform/internal/store/s3"
)

func main() {
	logger := logging.New()
	cfg, err := config.Load("execution-sandbox")
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	containerConfig := sandbox.ContainerConfig{SeccompProfile: os.Getenv("SANDBOX_SECCOMP_PROFILE")}
	var runner *sandbox.ContainerRunner
	switch cfg.SandboxRuntime {
	case sandbox.RuntimeGVisor:
		runner = sandbox.NewGVisorRunner(containerConfig)
	case sandbox.RuntimeRunc:
		logger.Warn("sandbox running on runc: local development only, no gVisor isolation")
		runner = sandbox.NewDockerRunner(containerConfig)
	default:
		logger.Error("SANDBOX_RUNTIME must be runsc or runc", slog.String("runtime", cfg.SandboxRuntime))
		os.Exit(1)
	}
	if n, err := runner.Reap(ctx); err != nil {
		logger.Error("reap leftover sandboxes", slog.Any("error", err))
		os.Exit(1)
	} else if n > 0 {
		logger.Info("reaped leftover sandboxes", slog.Int("count", n))
	}

	concurrency, err := strconv.Atoi(getenv("SANDBOX_CONCURRENCY", "4"))
	if err != nil || concurrency < 1 {
		logger.Error("SANDBOX_CONCURRENCY must be a positive integer")
		os.Exit(1)
	}
	snapshots := s3.New(s3.Config{Endpoint: cfg.S3Endpoint, Bucket: cfg.S3Bucket, AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey})
	service := sandbox.NewService(runner, snapshots, concurrency, logger)

	addr := cfg.HTTPAddr
	if os.Getenv("HTTP_ADDR") == "" {
		addr = ":8091"
	}
	server := &http.Server{Addr: addr, Handler: service.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), sandbox.MaxLimits.WallClock+10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("starting service", slog.String("service", cfg.ServiceName), slog.String("address", addr),
		slog.String("runtime", runner.Runtime()))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("serve execution sandbox", slog.Any("error", err))
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
