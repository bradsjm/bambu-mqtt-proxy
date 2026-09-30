// Shared HTTP service lifecycle: Stop must end streaming handlers through
// the canceled root context, fall back to a hard close for handlers that
// ignore cancellation, and stay safe before Start, after a failed Start,
// and on repeated calls.
package httpsrv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// reservePort binds an ephemeral loopback port and releases it for the test.
func reservePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// TestStopEndsStreamingHandlers proves the root-context cancellation: an
// SSE handler that waits on r.Context().Done() ends when Stop runs, and
// Stop returns without needing its hard-close fallback.
func TestStopEndsStreamingHandlers(t *testing.T) {
	port := reservePort(t)
	s := New(port, discardLogger())
	started := make(chan struct{})
	handlerDone := make(chan struct{})
	s.Mux().HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: hello\n\n")
		flusher.Flush()
		close(started)
		<-r.Context().Done()
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	first := []byte("data: hello\n\n")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprintf("http://127.0.0.1:%d/events", port), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatalf("read first event: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("streaming handler did not observe the canceled root context")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return")
	}
	// Once the handler returned, the connection must be closing on the
	// client side too.
	if _, err := resp.Body.Read(make([]byte, 1)); err == nil {
		t.Fatal("streaming response stayed open after Stop")
	}
}

// TestStopForceClosesHandlerIgnoringContext drives the Shutdown-timeout
// fallback: a handler that never observes its context forces the two-second
// budget to elapse, then Close takes over and the client connection ends.
func TestStopForceClosesHandlerIgnoringContext(t *testing.T) {
	port := reservePort(t)
	s := New(port, discardLogger())
	entered := make(chan struct{})
	release := make(chan struct{})
	s.Mux().HandleFunc("/stuck", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release // never observes r.Context(): only the hard close can end it
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	type result struct {
		resp *http.Response
		err  error
	}
	res := make(chan result, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/stuck", port))
		res <- result{resp: resp, err: err}
	}()
	<-entered

	started := time.Now()
	s.Stop()
	elapsed := time.Since(started)
	if elapsed < time.Second {
		t.Fatalf("Stop returned after %s; want the shutdown budget to elapse before the hard close", elapsed)
	}
	if elapsed > 6*time.Second {
		t.Fatalf("Stop took %s; want roughly the 2s shutdown budget", elapsed)
	}
	close(release)
	select {
	case r := <-res:
		if r.err != nil {
			return // connection killed before the response completed
		}
		defer r.resp.Body.Close()
		if _, err := r.resp.Body.Read(make([]byte, 1)); err == nil {
			t.Fatal("stuck connection survived the hard close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client request did not end after the hard close")
	}
}

// TestStopSafeBeforeStartAndRepeated covers the never-started and repeated
// paths: Stop must return promptly in both shapes.
func TestStopSafeBeforeStartAndRepeated(t *testing.T) {
	s := New(reservePort(t), discardLogger())
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung before Start")
	}
	s.Stop()
}

// TestStopSafeAfterFailedStart binds the port out from under Start, then
// requires Stop to be safe on the failed server and on a second call.
func TestStopSafeAfterFailedStart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold port: %v", err)
	}
	defer ln.Close()
	s := New(ln.Addr().(*net.TCPAddr).Port, discardLogger())
	if err := s.Start(); err == nil {
		t.Fatal("Start must fail while the port is held")
	}
	s.Stop()
	s.Stop()
}
