package metrics

import (
	"container/heap"
	"math"
	"math/rand"
	"sync"
	"time"
)

const rescaleThreshold = 1e9 * 60 * 60

// Samples maintain a statistically-significant selection of values from
// a stream.
type Sample interface {
	// Clear all samples.
	Clear()

	// Return the size of the sample, which is at most the reservoir size.
	Size() int

	// Update the sample with a new value.
	Update(value int64)

	// Return all the values in the sample.
	Values() []int64
}

// An exponentially-decaying sample using a forward-decaying priority
// reservoir.  See Cormode et al's "Forward Decay: A Practical Time Decay
// Model for Streaming Systems".
//
// <http://www.research.att.com/people/Cormode_Graham/library/publications/CormodeShkapenyukSrivastavaXu09.pdf>
type expDecaySample struct {
	alpha         float64
	mutex         sync.Mutex
	reservoirSize int
	t0, t1        time.Time
	values        expDecayIndividualSampleHeap
}

// Create a new exponentially-decaying sample with the given reservoir size
// and alpha.
func NewExpDecaySample(reservoirSize int, alpha float64) Sample {
	s := &expDecaySample{
		alpha:         alpha,
		reservoirSize: reservoirSize,
		t0:            time.Now(),
		values:        make(expDecayIndividualSampleHeap, 0, reservoirSize),
	}
	s.t1 = time.Now().Add(rescaleThreshold)
	return s
}

func (s *expDecaySample) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.values = make(expDecayIndividualSampleHeap, 0, s.reservoirSize)
	s.t0 = time.Now()
	s.t1 = s.t0.Add(rescaleThreshold)
}

func (s *expDecaySample) Size() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.values)
}

func (s *expDecaySample) Update(v int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(s.values) == s.reservoirSize {
		heap.Pop(&s.values)
	}
	t := time.Now()
	heap.Push(&s.values, expDecayIndividualSample{
		k: math.Exp(t.Sub(s.t0).Seconds()*s.alpha) / rand.Float64(),
		v: v,
	})
	if t.After(s.t1) {
		values := s.values
		t0 := s.t0
		s.values = make(expDecayIndividualSampleHeap, 0, s.reservoirSize)
		s.t0 = t
		s.t1 = s.t0.Add(rescaleThreshold)
		for _, v := range values {
			v.k = v.k * math.Exp(-s.alpha*float64(s.t0.Sub(t0)))
			heap.Push(&s.values, v)
		}
	}
}

func (s *expDecaySample) Values() []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values := make([]int64, len(s.values))
	for i, v := range s.values {
		values[i] = v.v
	}
	return values
}

// A uniform sample using Vitter's Algorithm R.
//
// <http://www.cs.umd.edu/~samir/498/vitter.pdf>
type uniformSample struct {
	mutex         sync.Mutex
	reservoirSize int
	values        []int64
}

// Create a new uniform sample with the given reservoir size.
func NewUniformSample(reservoirSize int) Sample {
	return &uniformSample{reservoirSize: reservoirSize}
}

func (s *uniformSample) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.values = make([]int64, 0, s.reservoirSize)
}

func (s *uniformSample) Size() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.values)
}

func (s *uniformSample) Update(v int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(s.values) < s.reservoirSize {
		s.values = append(s.values, v)
	} else {
		s.values[rand.Intn(s.reservoirSize)] = v
	}
}

func (s *uniformSample) Values() []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values := make([]int64, len(s.values))
	copy(values, s.values)
	return values
}

// An individual sample.
type expDecayIndividualSample struct {
	k float64
	v int64
}

// A min-heap of samples.
type expDecayIndividualSampleHeap []expDecayIndividualSample

func (q expDecayIndividualSampleHeap) Len() int {
	return len(q)
}

func (q expDecayIndividualSampleHeap) Less(i, j int) bool {
	return q[i].k < q[j].k
}

func (q *expDecayIndividualSampleHeap) Pop() interface{} {
	q_ := *q
	n := len(q_)
	i := q_[n-1]
	q_ = q_[0 : n-1]
	*q = q_
	return i
}

func (q *expDecayIndividualSampleHeap) Push(x interface{}) {
	q_ := *q
	n := len(q_)
	q_ = q_[0 : n+1]
	q_[n] = x.(expDecayIndividualSample)
	*q = q_
}

func (q expDecayIndividualSampleHeap) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}
