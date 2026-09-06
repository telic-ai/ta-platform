// Package apigen holds the Go server types and interfaces generated from
// api/openapi.yaml. Run `make generate` (or `go generate ./...`) to
// regenerate server.gen.go after changing the OpenAPI contract.
package apigen

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen --config=config.yaml ../../api/openapi.yaml
