package lifecycle

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestRunHTTPShutsDownAfterCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	server := &http.Server{Addr: listener.Addr().String(), Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})}
	done := make(chan error, 1)
	go func() {
		done <- RunHTTP(ctx, server, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	deadline := time.Now().Add(time.Second)
	for {
		response, requestErr := http.Get("http://" + server.Addr)
		if requestErr == nil {
			_ = response.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunHTTP() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunHTTP did not stop")
	}
}

func TestRunHTTPRejectsInvalidArguments(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := RunHTTP(context.Background(), nil, time.Second, logger); err == nil {
		t.Fatal("nil server accepted")
	}
	if err := RunHTTP(context.Background(), &http.Server{}, 0, logger); err == nil {
		t.Fatal("zero timeout accepted")
	}
	if err := RunHTTP(context.Background(), &http.Server{}, time.Second, nil); err == nil {
		t.Fatal("nil logger accepted")
	}
}
