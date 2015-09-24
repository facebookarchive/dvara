package metrics

import (
	"sync"
	"time"
)

// Meters count events to produce exponentially-weighted moving average rates
// at one-, five-, and fifteen-minutes and a mean rate.
type Meter interface {
	// Return the count of events seen.
	Count() int64

	// Mark the occurance of n events.
	Mark(n int64)

	// Tick the clock to update the moving average.
	Tick()

	// Return the meter's one-minute moving average rate of events.
	Rate1() float64

	// Return the meter's five-minute moving average rate of events.
	Rate5() float64

	// Return the meter's fifteen-minute moving average rate of events.
	Rate15() float64

	// Return the meter's mean rate of events.
	RateMean() float64
}

// Create a new meter.
type meter struct {
	mutex  sync.RWMutex
	count  int64
	rate1  EWMA
	rate5  EWMA
	rate15 EWMA
	start  time.Time
}

// Create a new meter.
func NewMeter() Meter {
	return &meter{
		rate1:  NewEWMA1(),
		rate5:  NewEWMA5(),
		rate15: NewEWMA15(),
		start:  time.Now(),
	}
}

func (m *meter) Count() int64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.count
}

func (m *meter) Mark(n int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.count += n
	m.rate1.Update(n)
	m.rate5.Update(n)
	m.rate15.Update(n)
}

func (m *meter) Tick() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.rate1.Tick()
	m.rate5.Tick()
	m.rate15.Tick()
}

func (m *meter) Rate1() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.rate1.Rate()
}

func (m *meter) Rate5() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.rate5.Rate()
}

func (m *meter) Rate15() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.rate15.Rate()
}

func (m *meter) RateMean() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return float64(1e9*m.count) / float64(time.Since(m.start))
}
