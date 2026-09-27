package byok

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

const currentStage = "AWSCURRENT"

// AWSSecrets reads company key envelopes from AWS Secrets Manager.
type AWSSecrets struct{ client *secretsmanager.Client }

func NewAWSSecrets(client *secretsmanager.Client) *AWSSecrets { return &AWSSecrets{client: client} }

func (s *AWSSecrets) Get(ctx context.Context, name string) (Secret, error) {
	out, err := s.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name), VersionStage: aws.String(currentStage),
	})
	if err != nil {
		return Secret{}, mapSecretsError(err)
	}
	value := out.SecretBinary
	if value == nil && out.SecretString != nil {
		value = []byte(*out.SecretString)
	}
	if len(value) == 0 {
		return Secret{}, ErrNoKey
	}
	return Secret{Value: value, VersionID: aws.ToString(out.VersionId)}, nil
}

// CurrentVersion uses DescribeSecret, which never returns the value.
func (s *AWSSecrets) CurrentVersion(ctx context.Context, name string) (string, error) {
	out, err := s.client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(name)})
	if err != nil {
		return "", mapSecretsError(err)
	}
	for version, stages := range out.VersionIdsToStages {
		for _, stage := range stages {
			if stage == currentStage {
				return version, nil
			}
		}
	}
	return "", ErrNoKey
}

func mapSecretsError(err error) error {
	var notFound *smtypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return ErrNoKey
	}
	return err
}

// AWSKMS generates and decrypts envelope data keys with AWS KMS.
type AWSKMS struct{ client *kms.Client }

func NewAWSKMS(client *kms.Client) *AWSKMS { return &AWSKMS{client: client} }

func (k *AWSKMS) GenerateDataKey(ctx context.Context, keyID string, encryptionContext map[string]string) ([]byte, []byte, error) {
	out, err := k.client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId: aws.String(keyID), KeySpec: kmstypes.DataKeySpecAes256, EncryptionContext: encryptionContext,
	})
	if err != nil {
		return nil, nil, err
	}
	return out.Plaintext, out.CiphertextBlob, nil
}

func (k *AWSKMS) Decrypt(ctx context.Context, encrypted []byte, encryptionContext map[string]string) ([]byte, error) {
	out, err := k.client.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: encrypted, EncryptionContext: encryptionContext})
	if err != nil {
		return nil, fmt.Errorf("kms decrypt: %w", err)
	}
	return out.Plaintext, nil
}
