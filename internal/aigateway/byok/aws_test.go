package byok

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// fakeAWS speaks the AWS JSON 1.1 protocol for the Secrets Manager and KMS
// operations the adapters use, backed by the in-memory fakes.
type fakeAWS struct {
	mu      sync.Mutex
	kms     *fakeKMS
	secrets map[string]struct {
		value   []byte
		version string
		asText  bool
	}
	targets []string
}

func (f *fakeAWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")
	f.mu.Lock()
	f.targets = append(f.targets, target)
	f.mu.Unlock()
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	reply := func(value any) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_ = json.NewEncoder(w).Encode(value)
	}
	fail := func(kind string) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"__type":"`+kind+`","message":"`+kind+`"}`)
	}
	encryptionContextOf := func() map[string]string {
		out := map[string]string{}
		for k, v := range body["EncryptionContext"].(map[string]any) {
			out[k] = v.(string)
		}
		return out
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch target {
	case "secretsmanager.GetSecretValue":
		secret, ok := f.secrets[body["SecretId"].(string)]
		if !ok {
			fail("ResourceNotFoundException")
			return
		}
		if body["VersionStage"] != "AWSCURRENT" {
			fail("InvalidRequestException")
			return
		}
		if secret.asText {
			reply(map[string]any{"SecretString": string(secret.value), "VersionId": secret.version})
		} else {
			reply(map[string]any{"SecretBinary": secret.value, "VersionId": secret.version})
		}
	case "secretsmanager.DescribeSecret":
		secret, ok := f.secrets[body["SecretId"].(string)]
		if !ok {
			fail("ResourceNotFoundException")
			return
		}
		reply(map[string]any{"VersionIdsToStages": map[string][]string{secret.version: {"AWSCURRENT"}, "older": {"AWSPREVIOUS"}}})
	case "TrentService.GenerateDataKey":
		if body["KeySpec"] != "AES_256" {
			fail("ValidationException")
			return
		}
		plaintext, encrypted, _ := f.kms.GenerateDataKey(r.Context(), body["KeyId"].(string), encryptionContextOf())
		reply(map[string]any{"Plaintext": plaintext, "CiphertextBlob": encrypted, "KeyId": body["KeyId"]})
	case "TrentService.Decrypt":
		raw, _ := json.Marshal(body["CiphertextBlob"])
		var blob []byte
		_ = json.Unmarshal(raw, &blob)
		plaintext, err := f.kms.Decrypt(r.Context(), blob, encryptionContextOf())
		if err != nil {
			fail("InvalidCiphertextException")
			return
		}
		reply(map[string]any{"Plaintext": plaintext})
	default:
		fail("UnknownOperationException")
	}
}

func (f *fakeAWS) setSecret(name string, value []byte, version string, asText bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.secrets == nil {
		f.secrets = map[string]struct {
			value   []byte
			version string
			asText  bool
		}{}
	}
	f.secrets[name] = struct {
		value   []byte
		version string
		asText  bool
	}{value, version, asText}
}

func awsClients(t *testing.T) (*fakeAWS, *AWSSecrets, *AWSKMS) {
	t.Helper()
	fake := &fakeAWS{kms: newFakeKMS()}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
		BaseEndpoint: aws.String(server.URL), RetryMaxAttempts: 1}
	return fake, NewAWSSecrets(secretsmanager.NewFromConfig(cfg)), NewAWSKMS(kms.NewFromConfig(cfg))
}

func TestAWSAdaptersEndToEnd(t *testing.T) {
	fake, secrets, kmsClient := awsClients(t)
	ctx := context.Background()
	resolver := NewResolver(secrets, kmsClient, Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	name := resolver.SecretName("company-1")

	sealed, err := Seal(ctx, kmsClient, "alias/ta-byok", "company-1", []byte("sk-ant-aws-1"))
	if err != nil {
		t.Fatalf("Seal via KMS: %v", err)
	}
	fake.setSecret(name, sealed, "v1", false)
	key, err := resolver.ResolveKey(ctx, "company-1")
	if err != nil || key.Reveal() != "sk-ant-aws-1" {
		t.Fatalf("ResolveKey = %v", err)
	}

	// Rotation through Secrets Manager, stored as SecretString this time.
	sealed, _ = Seal(ctx, kmsClient, "alias/ta-byok", "company-1", []byte("sk-ant-aws-2"))
	fake.setSecret(name, sealed, "v2", true)
	key, err = resolver.ResolveKey(ctx, "company-1")
	if err != nil || key.Reveal() != "sk-ant-aws-2" {
		t.Fatalf("after rotation: %v", err)
	}

	if _, err := resolver.ResolveKey(ctx, "company-2"); !errors.Is(err, ErrNoKey) {
		t.Errorf("missing secret err = %v, want ErrNoKey", err)
	}
	version, err := secrets.CurrentVersion(ctx, name)
	if err != nil || version != "v2" {
		t.Errorf("CurrentVersion = %q, %v", version, err)
	}
	if _, err := secrets.CurrentVersion(ctx, "nope"); !errors.Is(err, ErrNoKey) {
		t.Errorf("CurrentVersion missing err = %v", err)
	}

	// Another company's envelope must not decrypt under this company.
	fake.setSecret(resolver.SecretName("company-3"), sealed, "v1", false)
	if _, err := resolver.ResolveKey(ctx, "company-3"); err == nil {
		t.Error("company-3 opened company-1's envelope")
	}
	fake.mu.Lock()
	targets := fake.targets
	fake.mu.Unlock()
	seen := map[string]bool{}
	for _, target := range targets {
		seen[target] = true
	}
	for _, want := range []string{"secretsmanager.GetSecretValue", "secretsmanager.DescribeSecret", "TrentService.GenerateDataKey", "TrentService.Decrypt"} {
		if !seen[want] {
			t.Errorf("never called %s", want)
		}
	}
}
