// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"time"
)

// Config holds process-wide settings common to every service.
type Config struct {
	ServiceName string
	Env         string

	PostgresDSN   string
	KafkaBrokers  string
	RedisAddr     string
	ClickHouseDSN string
	S3Endpoint    string
	S3Bucket      string
	S3AccessKey   string
	S3SecretKey   string
	TypesenseURL  string
	TypesenseKey  string
	HTTPAddr      string
	SessionTTL    time.Duration
	// AIGatewayURL is where the Candidate Workspace reaches the AI Gateway.
	AIGatewayURL string
	// AIModel is the model the workspace requests; empty means the
	// gateway's default.
	AIModel string
	// SandboxURL is where the Candidate Workspace reaches the Execution
	// Sandbox; SandboxRuntime selects its runner (runsc or runc).
	SandboxURL     string
	SandboxRuntime string
	// RunTimeout is a candidate program's wall-clock limit.
	RunTimeout time.Duration
}

// Load reads configuration for serviceName from the environment, falling
// back to local-dev defaults that match deploy/docker-compose.yml.
func Load(serviceName string) (Config, error) {
	sessionTTL, err := time.ParseDuration(getenv("SESSION_TTL", "24h"))
	if err != nil || sessionTTL <= 0 {
		return Config{}, fmt.Errorf("SESSION_TTL must be a positive duration")
	}
	runTimeout, err := time.ParseDuration(getenv("RUN_TIMEOUT", "10s"))
	if err != nil || runTimeout <= 0 {
		return Config{}, fmt.Errorf("RUN_TIMEOUT must be a positive duration")
	}
	return Config{
		ServiceName: serviceName,
		Env:         getenv("ENV", "local"),

		PostgresDSN:    getenv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/ta_platform?sslmode=disable"),
		KafkaBrokers:   getenv("KAFKA_BROKERS", "localhost:9092"),
		RedisAddr:      getenv("REDIS_ADDR", "localhost:6379"),
		ClickHouseDSN:  getenv("CLICKHOUSE_DSN", "clickhouse://default:local-dev@localhost:9000/default"),
		S3Endpoint:     getenv("S3_ENDPOINT", "http://localhost:9100"),
		S3Bucket:       getenv("S3_BUCKET", "ta-platform"),
		S3AccessKey:    getenv("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:    getenv("S3_SECRET_KEY", "minioadmin"),
		TypesenseURL:   getenv("TYPESENSE_URL", "http://localhost:8108"),
		TypesenseKey:   getenv("TYPESENSE_API_KEY", "local-dev-key"),
		HTTPAddr:       getenv("HTTP_ADDR", ":8081"),
		SessionTTL:     sessionTTL,
		AIGatewayURL:   getenv("AI_GATEWAY_URL", "http://localhost:8090"),
		AIModel:        getenv("AI_MODEL", ""),
		SandboxURL:     getenv("SANDBOX_URL", "http://localhost:8091"),
		SandboxRuntime: getenv("SANDBOX_RUNTIME", "runsc"),
		RunTimeout:     runTimeout,
	}, nil
}

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
