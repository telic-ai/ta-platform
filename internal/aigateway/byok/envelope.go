// Package byok resolves a company's own provider API key ("bring your own
// key") for the AI Gateway.
//
// A key is stored envelope-encrypted: a KMS data key encrypts it with
// AES-256-GCM, and the envelope (KMS-encrypted data key, nonce and
// ciphertext) is the Secrets Manager secret value. The gateway decrypts
// the data key through KMS, opens the envelope in-process, and keeps the
// plaintext only in a short-lived in-memory cache that zeroes key bytes
// on eviction. The company ID is bound into both the KMS encryption
// context and the GCM additional data, so one company's envelope cannot be
// opened as another's.
//
// NOTE: [[Secrets Manager & KMS]] and [[AI Gateway – Internals]] were not
// available when this package was written; the envelope format and
// secret naming below are placeholders to reconcile against them.
package byok

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
)

const envelopeVersion = 1

// KMS is the part of AWS KMS the envelope uses.
type KMS interface {
	// GenerateDataKey returns a new 256-bit data key in plaintext and
	// encrypted under keyID with encryptionContext.
	GenerateDataKey(ctx context.Context, keyID string, encryptionContext map[string]string) (plaintext, encrypted []byte, err error)
	// Decrypt decrypts a data key; encryptionContext must match.
	Decrypt(ctx context.Context, encrypted []byte, encryptionContext map[string]string) ([]byte, error)
}

// Envelope is the stored form of a company's provider key.
type Envelope struct {
	Version          int    `json:"v"`
	KMSKeyID         string `json:"kms_key_id"`
	EncryptedDataKey []byte `json:"encrypted_data_key"`
	Nonce            []byte `json:"nonce"`
	Ciphertext       []byte `json:"ciphertext"`
}

func encryptionContext(companyID string) map[string]string {
	return map[string]string{"company_id": companyID, "purpose": "ai-provider-key"}
}

// Seal encrypts apiKey for companyID under the KMS key keyID and returns
// the serialized envelope to store as the secret value.
func Seal(ctx context.Context, kms KMS, keyID, companyID string, apiKey []byte) ([]byte, error) {
	dataKey, encryptedDataKey, err := kms.GenerateDataKey(ctx, keyID, encryptionContext(companyID))
	if err != nil {
		return nil, fmt.Errorf("byok: generate data key: %w", err)
	}
	defer zero(dataKey)
	aead, err := newAEAD(dataKey)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("byok: nonce: %w", err)
	}
	return json.Marshal(Envelope{
		Version: envelopeVersion, KMSKeyID: keyID, EncryptedDataKey: encryptedDataKey,
		Nonce: nonce, Ciphertext: aead.Seal(nil, nonce, apiKey, []byte(companyID)),
	})
}

// open decrypts a stored envelope for companyID. The caller owns, and must
// zero, the returned plaintext.
func open(ctx context.Context, kms KMS, companyID string, stored []byte) ([]byte, error) {
	var envelope Envelope
	if err := json.Unmarshal(stored, &envelope); err != nil {
		return nil, fmt.Errorf("byok: decode envelope: %w", err)
	}
	if envelope.Version != envelopeVersion {
		return nil, fmt.Errorf("byok: unsupported envelope version %d", envelope.Version)
	}
	dataKey, err := kms.Decrypt(ctx, envelope.EncryptedDataKey, encryptionContext(companyID))
	if err != nil {
		return nil, fmt.Errorf("byok: decrypt data key: %w", err)
	}
	defer zero(dataKey)
	aead, err := newAEAD(dataKey)
	if err != nil {
		return nil, err
	}
	if len(envelope.Nonce) != aead.NonceSize() {
		return nil, errors.New("byok: bad nonce")
	}
	plaintext, err := aead.Open(nil, envelope.Nonce, envelope.Ciphertext, []byte(companyID))
	if err != nil {
		return nil, errors.New("byok: envelope does not open for this company")
	}
	return plaintext, nil
}

func newAEAD(dataKey []byte) (cipher.AEAD, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("byok: data key is %d bytes, want 32", len(dataKey))
	}
	block, err := aes.NewCipher(dataKey)
	if err != nil {
		return nil, fmt.Errorf("byok: cipher: %w", err)
	}
	return cipher.NewGCM(block)
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
