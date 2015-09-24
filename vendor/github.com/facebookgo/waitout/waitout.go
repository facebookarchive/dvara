// Package waitout makes it easy to wait for some output.
package waitout

import (
	"bytes"
	"sync"
)

// Waiter waits for the expected byte sequence, once.
type Waiter struct {
	expected      []byte
	expectedMutex sync.Mutex
	actual        []byte
	done          bool
	cond          *sync.Cond
}

// Write checks if the written data contains the expected bytes and unblocks
// Wait.
func (w *Waiter) Write(p []byte) (int, error) {
	w.expectedMutex.Lock()
	defer w.expectedMutex.Unlock()
	w.actual = append(w.actual, p...)
	if bytes.Contains(w.actual, w.expected) {
		w.cond.L.Lock()
		defer w.cond.L.Unlock()
		w.done = true
		w.cond.Broadcast()
	}
	return len(p), nil
}

// Wait will block until the expected bytes are written.
func (w *Waiter) Wait() {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	for !w.done {
		w.cond.Wait()
	}
}

// New create a Waiter that will block on Wait until the expected bytes
// are written.
func New(expected []byte) *Waiter {
	w := &Waiter{expected: expected}
	w.cond = sync.NewCond(&sync.Mutex{})
	return w
}
