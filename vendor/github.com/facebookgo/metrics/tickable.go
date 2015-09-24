package metrics

import (
	"time"
)

// TickDuration defines the rate at which Tick() should get called for EWMA and
// other Tickable things that rely on EWMA like Meter & Timer.
const TickDuration = 5 * time.Second

// Tickable defines the interface implemented by metrics that need to Tick.
type Tickable interface {
	// Tick the clock to update the moving average.
	Tick()
}
