package metrics

import (
	"math"
	"sync"
	"sync/atomic"
)

// EWMAs continuously calculate an exponentially-weighted moving average
// based on an outside source of clock ticks.
type EWMA interface {
	// Return the moving average rate of events per second.
	Rate() float64

	// Tick the clock to update the moving average.
	Tick()

	// Add n uncounted events.
	Update(n int64)
}

// The standard implementation of an EWMA tracks the number of uncounted
// events and processes them on each tick.  It uses the sync/atomic package
// to manage uncounted events.
type ewma struct {
	alpha     float64
	rate      float64
	uncounted int64
	init      bool
	mutex     sync.RWMutex
}

// Create a new EWMA with the given alpha.
func NewEWMA(alpha float64) EWMA {
	return &ewma{alpha: alpha}
}

// Create a new EWMA with alpha set for a one-minute moving average.
func NewEWMA1() EWMA {
	return NewEWMA(1 - math.Exp(-5.0/60.0/1))
}

// Create a new EWMA with alpha set for a five-minute moving average.
func NewEWMA5() EWMA {
	return NewEWMA(1 - math.Exp(-5.0/60.0/5))
}

// Create a new EWMA with alpha set for a fifteen-minute moving average.
func NewEWMA15() EWMA {
	return NewEWMA(1 - math.Exp(-5.0/60.0/15))
}

func (a *ewma) Rate() float64 {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return a.rate * float64(1e9)
}

func (a *ewma) Tick() {
	count := atomic.LoadInt64(&a.uncounted)
	atomic.AddInt64(&a.uncounted, -count)
	instantRate := float64(count) / float64(TickDuration)
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.init {
		a.rate += a.alpha * (instantRate - a.rate)
	} else {
		a.init = true
		a.rate = instantRate
	}
}

func (a *ewma) Update(n int64) {
	atomic.AddInt64(&a.uncounted, n)
}
