// Package sse writes and reads text/event-stream responses.
package sse

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Event is one Server-Sent Event.
type Event struct {
	Name string
	Data []byte
}

// Writer emits events to an HTTP response, flushing after each one.
type Writer struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// NewWriter sets the event-stream headers and writes the 200 status. It
// fails if w cannot flush, since buffered SSE defeats its purpose.
func NewWriter(w http.ResponseWriter) (*Writer, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("sse: response writer does not support flushing")
	}
	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-store")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	return &Writer{w: w, flusher: flusher}, nil
}

// Send writes one event whose data is value encoded as JSON.
func (s *Writer) Send(name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("sse: encode %s: %w", name, err)
	}
	return s.SendRaw(name, data)
}

// SendRaw writes one event with pre-encoded data. Data must not contain
// newlines (JSON from encoding/json never does).
func (s *Writer) SendRaw(name string, data []byte) error {
	if strings.ContainsAny(name, "\r\n") {
		return errors.New("sse: event name contains a newline")
	}
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", name, data); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// Reader parses an event stream.
type Reader struct {
	scanner *bufio.Scanner
}

func NewReader(r io.Reader) *Reader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
	return &Reader{scanner: scanner}
}

// Next returns the next event, or io.EOF when the stream ends cleanly.
// Comment lines and fields other than event and data are ignored;
// multiple data lines are joined with newlines per the SSE spec.
func (r *Reader) Next() (Event, error) {
	var event Event
	var data []string
	seen := false
	for r.scanner.Scan() {
		line := r.scanner.Text()
		if line == "" {
			if seen {
				event.Data = []byte(strings.Join(data, "\n"))
				if event.Name == "" {
					event.Name = "message"
				}
				return event, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event.Name, seen = value, true
		case "data":
			data, seen = append(data, value), true
		}
	}
	if err := r.scanner.Err(); err != nil {
		return Event{}, err
	}
	if seen {
		return Event{}, io.ErrUnexpectedEOF
	}
	return Event{}, io.EOF
}
