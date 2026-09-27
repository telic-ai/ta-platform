package sse

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriterSetsHeadersAndFramesEvents(t *testing.T) {
	recorder := httptest.NewRecorder()
	writer, err := NewWriter(recorder)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := writer.Send("delta", map[string]string{"text": "hi\nthere"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q", got)
	}
	if !recorder.Flushed {
		t.Error("writer did not flush")
	}
	want := "event: delta\ndata: {\"text\":\"hi\\nthere\"}\n\n"
	if recorder.Body.String() != want {
		t.Errorf("body = %q, want %q", recorder.Body.String(), want)
	}
}

type noFlush struct{ http.ResponseWriter }

func TestNewWriterRequiresFlusher(t *testing.T) {
	if _, err := NewWriter(noFlush{httptest.NewRecorder()}); err == nil {
		t.Error("NewWriter accepted a writer that cannot flush")
	}
}

func TestSendRawRejectsNewlineInName(t *testing.T) {
	writer, _ := NewWriter(httptest.NewRecorder())
	if err := writer.SendRaw("bad\nname", []byte("{}")); err == nil {
		t.Error("SendRaw accepted an event name with a newline")
	}
}

func TestReaderParsesEvents(t *testing.T) {
	stream := ": keepalive\n\nevent: delta\ndata: {\"a\":1}\n\ndata: line1\ndata: line2\nid: 7\n\n"
	reader := NewReader(strings.NewReader(stream))
	first, err := reader.Next()
	if err != nil || first.Name != "delta" || string(first.Data) != `{"a":1}` {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := reader.Next()
	if err != nil || second.Name != "message" || string(second.Data) != "line1\nline2" {
		t.Fatalf("second = %+v, %v", second, err)
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end err = %v, want EOF", err)
	}
}

func TestReaderReportsTruncatedEvent(t *testing.T) {
	reader := NewReader(strings.NewReader("event: delta\ndata: {}"))
	if _, err := reader.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}
