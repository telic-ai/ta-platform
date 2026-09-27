package byok

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
)

// fakeKMS wraps data keys with an in-memory master key and enforces the
// encryption context like KMS does.
type fakeKMS struct {
	master   cipher.AEAD
	decrypts atomic.Int32
	err      error
}

func newFakeKMS() *fakeKMS {
	masterKey := make([]byte, 32)
	_, _ = rand.Read(masterKey)
	block, _ := aes.NewCipher(masterKey)
	aead, _ := cipher.NewGCM(block)
	return &fakeKMS{master: aead}
}

func contextAAD(encryptionContext map[string]string) []byte {
	var aad []byte
	for _, k := range []string{"company_id", "purpose"} {
		aad = append(aad, k+"="+encryptionContext[k]+";"...)
	}
	return aad
}

func (k *fakeKMS) GenerateDataKey(_ context.Context, _ string, encryptionContext map[string]string) ([]byte, []byte, error) {
	dataKey := make([]byte, 32)
	_, _ = rand.Read(dataKey)
	nonce := make([]byte, k.master.NonceSize())
	_, _ = rand.Read(nonce)
	return dataKey, append(nonce, k.master.Seal(nil, nonce, dataKey, contextAAD(encryptionContext))...), nil
}

func (k *fakeKMS) Decrypt(_ context.Context, encrypted []byte, encryptionContext map[string]string) ([]byte, error) {
	k.decrypts.Add(1)
	if k.err != nil {
		return nil, k.err
	}
	n := k.master.NonceSize()
	if len(encrypted) < n {
		return nil, errors.New("InvalidCiphertextException")
	}
	plaintext, err := k.master.Open(nil, encrypted[:n], encrypted[n:], contextAAD(encryptionContext))
	if err != nil {
		return nil, errors.New("InvalidCiphertextException")
	}
	return plaintext, nil
}

// fakeSecrets is an in-memory Secrets Manager with versions.
type fakeSecrets struct {
	mu            sync.Mutex
	secrets       map[string]Secret
	gets          atomic.Int32
	versionChecks atomic.Int32
}

func (s *fakeSecrets) put(name string, value []byte, version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.secrets == nil {
		s.secrets = map[string]Secret{}
	}
	s.secrets[name] = Secret{Value: value, VersionID: version}
}

func (s *fakeSecrets) remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.DeleteFunc(s.secrets, func(k string, _ Secret) bool { return k == name })
}

func (s *fakeSecrets) Get(_ context.Context, name string) (Secret, error) {
	s.gets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	secret, ok := s.secrets[name]
	if !ok {
		return Secret{}, ErrNoKey
	}
	return secret, nil
}

func (s *fakeSecrets) CurrentVersion(_ context.Context, name string) (string, error) {
	s.versionChecks.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	secret, ok := s.secrets[name]
	if !ok {
		return "", ErrNoKey
	}
	return secret.VersionID, nil
}
