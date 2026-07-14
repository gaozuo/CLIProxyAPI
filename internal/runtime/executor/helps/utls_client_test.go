package helps

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type utlsClientRoundTripFunc func(*http.Request) (*http.Response, error)

func (f utlsClientRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type controlledUtlsDialer struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
	calls       atomic.Int32
}

func newControlledUtlsDialer() *controlledUtlsDialer {
	return &controlledUtlsDialer{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (d *controlledUtlsDialer) markStarted() {
	d.calls.Add(1)
	d.startedOnce.Do(func() { close(d.started) })
}

func (d *controlledUtlsDialer) unblock() {
	d.releaseOnce.Do(func() { close(d.release) })
}

func (d *controlledUtlsDialer) Dial(string, string) (net.Conn, error) {
	d.markStarted()
	<-d.release
	return nil, errors.New("dial released")
}

func (d *controlledUtlsDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	d.markStarted()
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-d.release:
		return nil, errors.New("dial released")
	}
}

type handshakeUtlsDialer struct {
	client  net.Conn
	started chan struct{}
	once    sync.Once
}

func (d *handshakeUtlsDialer) dial() (net.Conn, error) {
	d.once.Do(func() { close(d.started) })
	return d.client, nil
}

func (d *handshakeUtlsDialer) Dial(string, string) (net.Conn, error) {
	return d.dial()
}

func (d *handshakeUtlsDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return d.dial()
}

func TestNewUtlsHTTPClientUsesContextRoundTripperForProtectedHost(t *testing.T) {
	t.Parallel()

	called := false
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", utlsClientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		if req.URL.Hostname() != "chatgpt.com" {
			t.Fatalf("hostname = %q, want chatgpt.com", req.URL.Hostname())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}))

	client := NewUtlsHTTPClient(ctx, nil, nil, 0)
	resp, err := client.Get("https://chatgpt.com/backend-api/codex/responses")
	if err != nil {
		t.Fatalf("client.Get returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response body close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected context RoundTripper to handle protected host request")
	}
}

func TestUtlsRoundTripperCancelsProtectedHostDuringDial(t *testing.T) {
	dialer := newControlledUtlsDialer()
	defer dialer.unblock()
	roundTripper := newUtlsRoundTripper("")
	roundTripper.dialer = dialer

	ctx, cancel := context.WithCancel(context.Background())
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequestWithContext returned error: %v", errRequest)
	}

	done := make(chan error, 1)
	go func() {
		_, errRoundTrip := roundTripper.RoundTrip(req)
		done <- errRoundTrip
	}()

	select {
	case <-dialer.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for protected-host dial")
	}
	cancel()

	select {
	case errRoundTrip := <-done:
		if !errors.Is(errRoundTrip, context.Canceled) {
			t.Fatalf("RoundTrip error = %v, want context canceled", errRoundTrip)
		}
	case <-time.After(250 * time.Millisecond):
		dialer.unblock()
		<-done
		t.Fatal("protected-host dial ignored request cancellation")
	}
}

func TestUtlsRoundTripperCancelsProtectedHostDuringPendingWait(t *testing.T) {
	dialer := newControlledUtlsDialer()
	defer dialer.unblock()
	roundTripper := newUtlsRoundTripper("")
	roundTripper.dialer = dialer

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstReq, errFirstRequest := http.NewRequestWithContext(firstCtx, http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errFirstRequest != nil {
		t.Fatalf("first request creation returned error: %v", errFirstRequest)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, errRoundTrip := roundTripper.RoundTrip(firstReq)
		firstDone <- errRoundTrip
	}()

	select {
	case <-dialer.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first protected-host dial")
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondReq, errSecondRequest := http.NewRequestWithContext(secondCtx, http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errSecondRequest != nil {
		t.Fatalf("second request creation returned error: %v", errSecondRequest)
	}
	secondDone := make(chan error, 1)
	go func() {
		_, errRoundTrip := roundTripper.RoundTrip(secondReq)
		secondDone <- errRoundTrip
	}()
	cancelSecond()

	select {
	case errRoundTrip := <-secondDone:
		if !errors.Is(errRoundTrip, context.Canceled) {
			t.Fatalf("second RoundTrip error = %v, want context canceled", errRoundTrip)
		}
	case <-time.After(250 * time.Millisecond):
		dialer.unblock()
		<-secondDone
		<-firstDone
		t.Fatal("pending protected-host request ignored cancellation")
	}

	if got := dialer.calls.Load(); got != 1 {
		t.Fatalf("dial calls = %d, want 1 while second request waits", got)
	}
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("timed out cleaning up first protected-host request")
	}
}

func TestUtlsRoundTripperCancelsProtectedHostDuringTLSHandshake(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	dialer := &handshakeUtlsDialer{client: clientConn, started: make(chan struct{})}
	roundTripper := newUtlsRoundTripper("")
	roundTripper.dialer = dialer

	ctx, cancel := context.WithCancel(context.Background())
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequestWithContext returned error: %v", errRequest)
	}
	done := make(chan error, 1)
	go func() {
		_, errRoundTrip := roundTripper.RoundTrip(req)
		done <- errRoundTrip
	}()

	select {
	case <-dialer.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for protected-host TLS handshake")
	}
	cancel()

	select {
	case errRoundTrip := <-done:
		if !errors.Is(errRoundTrip, context.Canceled) {
			t.Fatalf("RoundTrip error = %v, want context canceled", errRoundTrip)
		}
	case <-time.After(250 * time.Millisecond):
		_ = serverConn.Close()
		<-done
		t.Fatal("protected-host TLS handshake ignored request cancellation")
	}
}
