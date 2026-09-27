// Package s3 provides the shared object storage client for services,
// pointed at MinIO locally and S3 in deployed environments.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ErrNotFound means the object does not exist.
var ErrNotFound = errors.New("s3: object not found")

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

// Put stores body at key in the client's bucket.
func (c *Client) Put(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := c.raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("s3: put %q: %w", key, err)
	}
	return nil
}

// Get reads the object at key, refusing objects larger than maxBytes.
func (c *Client) Get(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	out, err := c.raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("s3: read %q: %w", key, err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("s3: object %q exceeds %d bytes", key, maxBytes)
	}
	return body, nil
}

// EnsureBucket creates the client's bucket if it does not exist.
func (c *Client) EnsureBucket(ctx context.Context) error {
	if c.Ping(ctx) == nil {
		return nil
	}
	_, err := c.raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.bucket)})
	var owned *types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) {
		return fmt.Errorf("s3: create bucket %q: %w", c.bucket, err)
	}
	return nil
}
