package metrics

import "sync/atomic"

// Counters hold an int64 value that can be incremented and decremented.
type Counter interface {
	// Clear the counter: set it to zero.
	Clear()

	// Return the current count.
	Count() int64

	// Decrement the counter by the given amount.
	Dec(amount int64) Counter

	// Increment the counter by the given amount.
	Inc(amount int64) Counter
}

// The standard implementation of a Counter uses the sync/atomic package
// to manage a single int64 value.
type counter struct {
	count int64
}

// Create a new counter.
func NewCounter() Counter {
	return &counter{count: 0}
}

func (c *counter) Clear() {
	atomic.StoreInt64(&c.count, 0)
}

func (c *counter) Count() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *counter) Dec(i int64) Counter {
	atomic.AddInt64(&c.count, -i)
	return c
}

func (c *counter) Inc(i int64) Counter {
	atomic.AddInt64(&c.count, i)
	return c
}
