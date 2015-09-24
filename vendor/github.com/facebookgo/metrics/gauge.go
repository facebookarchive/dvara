package metrics

import "sync/atomic"

// Gauges hold an int64 value that can be set arbitrarily.
type Gauge interface {
	// Update the gauge's value.
	Update(value int64)

	// Return the gauge's current value.
	Value() int64
}

// The standard implementation of a Gauge uses the sync/atomic package
// to manage a single int64 value.
type gauge struct {
	value int64
}

// Create a new gauge.
func NewGauge() Gauge {
	return &gauge{0}
}

func (g *gauge) Update(v int64) {
	atomic.StoreInt64(&g.value, v)
}

func (g *gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}
