// Package s3 provides the shared object storage client for services,
// pointed at MinIO locally and S3 in deployed environments.
package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds the fields needed to reach an S3-compatible endpoint.
type Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// Client wraps an S3-compatible client scoped to a single bucket.
type Client struct {
	raw    *s3.Client
	bucket string
}

// New builds a client from cfg (see internal/platform/config).
func New(cfg Config) *Client {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	raw := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(cfg.Endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		UsePathStyle: true,
	})

	return &Client{raw: raw, bucket: cfg.Bucket}
}

// Raw returns the underlying AWS SDK client for direct API access.
func (c *Client) Raw() *s3.Client {
	return c.raw
}

// Ping verifies connectivity by checking that the configured bucket exists.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.raw.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.bucket)})
	if err != nil {
		return fmt.Errorf("s3: head bucket %q: %w", c.bucket, err)
	}
	return nil
}
