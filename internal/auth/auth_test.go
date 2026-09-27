package auth

import (
	"net/http/httptest"
	"testing"
)

func TestBearerToken(t *testing.T) {
	for header, want := range map[string]string{
		"Bearer abc": "abc",
		"bearer abc": "abc",
		"Basic abc":  "",
		"Bearer":     "",
		"Bearer a b": "",
		"":           "",
	} {
		request := httptest.NewRequest("GET", "/", nil)
		request.Header.Set("Authorization", header)
		got, ok := BearerToken(request)
		if got != want || ok != (want != "") {
			t.Errorf("BearerToken(%q) = %q, %v; want %q", header, got, ok, want)
		}
	}
}
