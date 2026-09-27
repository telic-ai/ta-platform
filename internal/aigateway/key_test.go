package aigateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestKeyNeverRenders(t *testing.T) {
	key := NewKey([]byte("sk-live-very-secret"))
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	logger.Info("resolved", slog.Any("key", key), "raw", key)
	encoded, _ := json.Marshal(struct{ Key *Key }{key})
	rendered := fmt.Sprintf("%v %+v %s %#v", key, key, key, key) + logs.String() + string(encoded)
	if strings.Contains(rendered, "very-secret") {
		t.Fatalf("key rendered: %s", rendered)
	}
	if !strings.Contains(rendered, redacted) {
		t.Errorf("expected redaction marker in %s", rendered)
	}
	if key.Reveal() != "sk-live-very-secret" {
		t.Error("Reveal returned the wrong value")
	}
}

func TestNewKeyCopiesAndZeroOverwrites(t *testing.T) {
	secret := []byte("sk-abc")
	key := NewKey(secret)
	secret[0] = 'X'
	if key.Reveal() != "sk-abc" {
		t.Fatal("NewKey did not copy its input")
	}
	backing := key.value
	key.Zero()
	if !key.Zeroed() || key.Reveal() != "" {
		t.Error("key still readable after Zero")
	}
	for _, b := range backing {
		if b != 0 {
			t.Fatalf("backing bytes not zeroed: %v", backing)
		}
	}
	var nilKey *Key
	nilKey.Zero()
	if nilKey.Reveal() != "" {
		t.Error("nil key revealed a value")
	}
}
