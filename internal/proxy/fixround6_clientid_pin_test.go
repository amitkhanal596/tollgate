package proxy

import (
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// metrics may not import config, so the two limits are kept equal by hand; a
// log cap below the config cap would truncate valid client IDs in the access log.
func TestAccessLogClientIDCapCoversConfigLimit(t *testing.T) {
	if metrics.MaxLogClientIDBytes < config.MaxClientIDLen {
		t.Fatalf("metrics.MaxLogClientIDBytes = %d < config.MaxClientIDLen = %d",
			metrics.MaxLogClientIDBytes, config.MaxClientIDLen)
	}
}
