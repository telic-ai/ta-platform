package byok

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	kms := newFakeKMS()
	stored, err := Seal(context.Background(), kms, "alias/byok", "company-a", []byte("sk-ant-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte("sk-ant-secret")) {
		t.Fatal("envelope contains the plaintext key")
	}
	plaintext, err := open(context.Background(), kms, "company-a", stored)
	if err != nil || string(plaintext) != "sk-ant-secret" {
		t.Fatalf("open = %q, %v", plaintext, err)
	}
	var envelope Envelope
	_ = json.Unmarshal(stored, &envelope)
	if envelope.Version != 1 || envelope.KMSKeyID != "alias/byok" || len(envelope.Nonce) != 12 {
		t.Errorf("envelope = %+v", envelope)
	}
}

func TestOpenIsBoundToCompany(t *testing.T) {
	kms := newFakeKMS()
	stored, _ := Seal(context.Background(), kms, "k", "company-a", []byte("sk"))
	if _, err := open(context.Background(), kms, "company-b", stored); err == nil {
		t.Error("company B opened company A's envelope")
	}
}

func TestOpenRejectsTampering(t *testing.T) {
	kms := newFakeKMS()
	stored, _ := Seal(context.Background(), kms, "k", "c", []byte("sk-original"))
	var envelope Envelope
	_ = json.Unmarshal(stored, &envelope)

	tampered := envelope
	tampered.Ciphertext = append([]byte{}, envelope.Ciphertext...)
	tampered.Ciphertext[0] ^= 1
	bad, _ := json.Marshal(tampered)
	if _, err := open(context.Background(), kms, "c", bad); err == nil {
		t.Error("tampered ciphertext opened")
	}
	version := envelope
	version.Version = 9
	bad, _ = json.Marshal(version)
	if _, err := open(context.Background(), kms, "c", bad); err == nil {
		t.Error("unknown version opened")
	}
	nonce := envelope
	nonce.Nonce = []byte{1}
	bad, _ = json.Marshal(nonce)
	if _, err := open(context.Background(), kms, "c", bad); err == nil {
		t.Error("short nonce accepted")
	}
	if _, err := open(context.Background(), kms, "c", []byte("{")); err == nil {
		t.Error("garbage accepted")
	}
}

func TestNewAEADRequires256BitKey(t *testing.T) {
	if _, err := newAEAD(make([]byte, 16)); err == nil {
		t.Error("128-bit data key accepted")
	}
}
