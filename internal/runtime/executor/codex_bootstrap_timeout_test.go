package executor

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestCodexBootstrapTimeoutBounds(t *testing.T) {
	if got := codexBootstrapTimeout(nil); got != 0 {
		t.Fatalf("nil config timeout = %s, want 0", got)
	}
	if got := codexBootstrapTimeout(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{BootstrapTimeoutSeconds: 20}}}); got != 20*time.Second {
		t.Fatalf("timeout = %s, want 20s", got)
	}
	if got := codexBootstrapTimeout(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{BootstrapTimeoutSeconds: 9999}}}); got != 10*time.Minute {
		t.Fatalf("capped timeout = %s, want 10m", got)
	}
}
