// Package rpool provides a resource pool.
package rpool

import (
	"container/list"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/facebookgo/clock"
	"github.com/facebookgo/stats"
)

var (
	errPoolClosed  = errors.New("rpool: pool has been closed")
	errCloseAgain  = errors.New("rpool: Pool.Close called more than once")
	errWrongPool   = errors.New("rpool: provided resource was not acquired from this pool")
	closedSentinel = sentinelCloser(1)
	newSentinel    = sentinelCloser(2)
)

// Pool manages the life cycle of resources.
type Pool struct {
	// New is used to create a new resource when necessary.
	New func() (io.Closer, error)

	// CloseErrorHandler will be called when an error occurs closing a resource.
	CloseErrorHandler func(err error)

	// Stats is optional and allows for the pool to provide stats for various
	// interesting events in the pool.
	Stats stats.Client

	// Max defines the maximum number of concurrently allocated resources.
	Max uint

	// MinIdle defines the number of minimum idle resources. These number of
	// resources are kept around when the idle cleanup kicks in.
	MinIdle uint

	// IdleTimeout defines the duration of idle time after which a resource will
	// be closed.
	IdleTimeout time.Duration

	// ClosePoolSize defines the number of concurrent goroutines that will close
	// resources.
	ClosePoolSize uint

	// Clock allows for testing timing related functionality. Do not specify this
	// in production code.
	Clock clock.Clock

	manageOnce sync.Once
	acquire    chan chan io.Closer
	new        chan io.Closer
	release    chan returnResource
	discard    chan returnResource
	close      chan chan error
}

// Acquire will pull a resource from the pool or create a new one if necessary.
func (p *Pool) Acquire() (io.Closer, error) {
	defer stats.BumpTime(p.Stats, "acquire.time").End()
	p.manageOnce.Do(p.goManage)
	r := make(chan io.Closer)
	p.acquire <- r
	c := <-r

	// sentinel value indicates the pool is closed
	if c == closedSentinel {
		return nil, errPoolClosed
	}

	// need to allocate a new resource
	if c == newSentinel {
		t := stats.BumpTime(p.Stats, "acquire.new.time")
		c, err := p.New()
		t.End()
		stats.BumpSum(p.Stats, "acquire.new", 1)
		if err != nil {
			stats.BumpSum(p.Stats, "acquire.error.new", 1)
			// discard our assumed checked out resource since we failed to New
			p.discard <- returnResource{resource: newSentinel}
		} else {
			p.new <- c
		}
		return c, err
	}

	// successfully acquired from pool
	return c, nil
}

// Release puts the resource back into the pool. It will panic if you try to
// release a resource that wasn't acquired from this pool.
func (p *Pool) Release(c io.Closer) {
	p.manageOnce.Do(p.goManage)

	// we get an error back and panic here instead of the manage function so the
	// caller gets the panic and we end up with a useful stack.
	res := make(chan error)
	p.release <- returnResource{resource: c, response: res}
	if err := <-res; err != nil {
		panic(err)
	}
}

// Discard closes the resource and indicates we're throwing it away. It will
// panic if you try to discard a resource that wasn't acquired from this pool.
func (p *Pool) Discard(c io.Closer) {
	p.manageOnce.Do(p.goManage)

	// we get an error back and panic here instead of the manage function so the
	// caller gets the panic and we end up with a useful stack.
	res := make(chan error)
	p.discard <- returnResource{resource: c, response: res}
	if err := <-res; err != nil {
		panic(err)
	}
}

// Close closes the pool and its resources. It waits until all acquired
// resources are released or discarded. It is an error to call Acquire after
// closing the pool.
func (p *Pool) Close() error {
	defer stats.BumpTime(p.Stats, "shutdown.time").End()
	p.manageOnce.Do(p.goManage)
	r := make(chan error)
	p.close <- r
	return <-r
}

func (p *Pool) goManage() {
	if p.Max == 0 {
		panic("no max configured")
	}
	if p.IdleTimeout.Nanoseconds() == 0 {
		panic("no idle timeout configured")
	}
	if p.ClosePoolSize == 0 {
		panic("no close pool size configured")
	}

	p.acquire = make(chan chan io.Closer)
	p.new = make(chan io.Closer)
	p.release = make(chan returnResource)
	p.discard = make(chan returnResource)
	p.close = make(chan chan error)
	go p.manage()
}

type entry struct {
	resource io.Closer
	use      time.Time
}

func (p *Pool) manage() {
	klock := p.Clock
	if klock == nil {
		klock = clock.New()
	}

	// setup goroutines to close resources
	closers := make(chan io.Closer)
	var closeWG sync.WaitGroup
	closeWG.Add(int(p.ClosePoolSize))
	for i := uint(0); i < p.ClosePoolSize; i++ {
		go func() {
			defer closeWG.Done()
			for c := range closers {
				t := stats.BumpTime(p.Stats, "close.time")
				stats.BumpSum(p.Stats, "close", 1)
				if err := c.Close(); err != nil {
					stats.BumpSum(p.Stats, "close.error", 1)
					p.CloseErrorHandler(err)
				}
				t.End()
			}
		}()
	}

	// setup a ticker to report various averages every minute. if we don't have a
	// Stats implementation provided, we Stop it so it never ticks.
	statsTicker := klock.Ticker(time.Minute)
	if p.Stats == nil {
		statsTicker.Stop()
	}

	resources := []entry{}
	outResources := map[io.Closer]struct{}{}
	out := uint(0)
	waiting := list.New()
	idleTicker := klock.Ticker(p.IdleTimeout)
	closed := false
	var closeResponse chan error
	for {
		if closed && out == 0 && waiting.Len() == 0 {
			if p.Stats != nil {
				statsTicker.Stop()
			}

			// all waiting acquires are done, all resources have been released.
			// now just wait for all resources to close.
			close(closers)
			closeWG.Wait()

			// close internal channels.
			close(p.acquire)
			close(p.new)
			close(p.release)
			close(p.discard)
			close(p.close)

			// return a response to the original close.
			closeResponse <- nil

			return
		}

		select {
		case r := <-p.acquire:
			// if closed, new acquire calls are rejected
			if closed {
				r <- closedSentinel
				stats.BumpSum(p.Stats, "acquire.error.closed", 1)
				continue
			}

			// acquire from pool
			if cl := len(resources); cl > 0 {
				c := resources[cl-1]
				outResources[c.resource] = struct{}{}
				r <- c.resource
				resources = resources[:cl-1]
				out++
				stats.BumpSum(p.Stats, "acquire.pool", 1)
				continue
			}

			// max resources already in use, need to block & wait
			if out == p.Max {
				waiting.PushBack(r)
				stats.BumpSum(p.Stats, "acquire.waiting", 1)
				continue
			}

			// Make a new resource in the calling goroutine by sending it a
			// newSentinel. We assume it's checked out. Acquire will discard if
			// creating a new resource fails.
			out++
			r <- newSentinel
		case c := <-p.new:
			outResources[c] = struct{}{}
		case rr := <-p.release:
			// ensure we're dealing with a resource acquired thru us
			if _, found := outResources[rr.resource]; !found {
				rr.response <- errWrongPool
				return
			}
			close(rr.response)

			// pass it to someone who's waiting
			if e := waiting.Front(); e != nil {
				r := waiting.Remove(e).(chan io.Closer)
				r <- rr.resource
				continue
			}

			// no longer out
			out--
			delete(outResources, rr.resource)

			// no one is waiting, and we're closed, schedule it to be closed
			if closed {
				closers <- rr.resource
				continue
			}

			// put it back in our pool
			resources = append(resources, entry{resource: rr.resource, use: klock.Now()})
		case rr := <-p.discard:
			// ensure we're dealing with a resource acquired thru us
			if rr.resource != newSentinel { // this happens when new fails
				if _, found := outResources[rr.resource]; !found {
					rr.response <- errWrongPool
					return
				}
				close(rr.response)
				delete(outResources, rr.resource)
				closers <- rr.resource
			}

			// we can make a new one if someone is waiting. no need to decrement out
			// in this case since we assume this new one is checked out. Acquire will
			// discard if creating a new resource fails.
			if e := waiting.Front(); e != nil {
				r := waiting.Remove(e).(chan io.Closer)
				r <- newSentinel
				continue
			}

			// otherwise we lost a resource and dont need a new one right away
			out--
		case now := <-idleTicker.C:
			eligibleOffset := len(resources) - int(p.MinIdle)

			// less than min idle, nothing to do
			if eligibleOffset <= 0 {
				continue
			}

			t := stats.BumpTime(p.Stats, "idle.cleanup.time")

			// cleanup idle resources
			idleLen := 0
			for _, e := range resources[:eligibleOffset] {
				if now.Sub(e.use) < p.IdleTimeout {
					break
				}
				closers <- e.resource
				idleLen++
			}

			// move the remaining resources to the beginning
			resources = resources[:copy(resources, resources[idleLen:])]

			t.End()
			stats.BumpSum(p.Stats, "idle.closed", float64(idleLen))
		case <-statsTicker.C:
			// We can assume if we hit this then p.Stats is not nil
			p.Stats.BumpAvg("waiting", float64(waiting.Len()))
			p.Stats.BumpAvg("idle", float64(len(resources)))
			p.Stats.BumpAvg("out", float64(out))
			p.Stats.BumpAvg("alive", float64(uint(len(resources))+out))
		case r := <-p.close:
			// cant call close if already closing
			if closed {
				r <- errCloseAgain
				continue
			}

			closed = true
			idleTicker.Stop() // stop idle processing

			// close idle since if we have idle, implicitly no one is waiting
			for _, e := range resources {
				closers <- e.resource
			}

			closeResponse = r
		}
	}
}

type returnResource struct {
	resource io.Closer
	response chan error
}

type sentinelCloser int

func (s sentinelCloser) Close() error {
	panic("should never get called")
}
