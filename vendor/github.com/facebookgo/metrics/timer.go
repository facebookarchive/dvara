package metrics

import "time"

// Timers capture the duration and rate of events.
type Timer interface {
	// Return the count of inputs.
	Count() int64

	// Return the maximal value seen.
	Max() int64

	// Return the mean of all values seen.
	Mean() float64

	// Return the minimal value seen.
	Min() int64

	// Return an arbitrary percentile of all values seen.
	Percentile(p float64) float64

	// Return a slice of arbitrary percentiles of all values seen.
	Percentiles(ps []float64) []float64

	// Return the meter's one-minute moving average rate of events.
	Rate1() float64

	// Return the meter's five-minute moving average rate of events.
	Rate5() float64

	// Return the meter's fifteen-minute moving average rate of events.
	Rate15() float64

	// Return the meter's mean rate of events.
	RateMean() float64

	// Return the standard deviation of all values seen.
	StdDev() float64

	// Start captures the current time and returns a value which implements Stop
	// to log the elapsed time. It should be used like:
	//
	//     defer timer.Start().Stop()
	Start() interface {
		Stop()
	}

	// Record the duration of an event.
	Update(d time.Duration)

	// Record the duration of an event that started at a time and ends now.
	UpdateSince(t time.Time)

	// Tick the clock to update the moving average.
	Tick()
}

type capture struct {
	timer Timer
	start time.Time
}

func (c *capture) Stop() {
	c.timer.UpdateSince(c.start)
}

// The standard implementation of a Timer uses a Histogram and Meter directly.
type timer struct {
	h Histogram
	m Meter
}

// Create a new timer with the given Histogram and Meter.
func NewCustomTimer(h Histogram, m Meter) Timer {
	return &timer{h, m}
}

// Create a new timer with a standard histogram and meter.  The histogram
// will use an exponentially-decaying sample with the same reservoir size
// and alpha as UNIX load averages.
func NewTimer() Timer {
	return &timer{
		NewHistogram(NewExpDecaySample(1028, 0.015)),
		NewMeter(),
	}
}

func (t *timer) Count() int64 {
	return t.h.Count()
}

func (t *timer) Max() int64 {
	return t.h.Max()
}

func (t *timer) Mean() float64 {
	return t.h.Mean()
}

func (t *timer) Min() int64 {
	return t.h.Min()
}

func (t *timer) Percentile(p float64) float64 {
	return t.h.Percentile(p)
}

func (t *timer) Percentiles(ps []float64) []float64 {
	return t.h.Percentiles(ps)
}

func (t *timer) Rate1() float64 {
	return t.m.Rate1()
}

func (t *timer) Rate5() float64 {
	return t.m.Rate5()
}

func (t *timer) Rate15() float64 {
	return t.m.Rate15()
}

func (t *timer) RateMean() float64 {
	return t.m.RateMean()
}

func (t *timer) StdDev() float64 {
	return t.h.StdDev()
}

func (t *timer) Start() interface {
	Stop()
} {
	return &capture{
		start: time.Now(),
		timer: t,
	}
}

func (t *timer) Update(d time.Duration) {
	t.h.Update(int64(d))
	t.m.Mark(1)
}

func (t *timer) UpdateSince(ts time.Time) {
	t.h.Update(int64(time.Since(ts)))
	t.m.Mark(1)
}

func (t *timer) Tick() {
	t.m.Tick()
}
