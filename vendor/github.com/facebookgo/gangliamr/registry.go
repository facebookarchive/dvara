package gangliamr

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/facebookgo/ganglia/gmetric"
	"github.com/facebookgo/metrics"
)

// Internally we verify the registered metrics match this interface. The basic
// design of the metric in this library is to map a single metric from
// go.metrics to one or more gmetric.Metrics. Each of those metrics must be
// registered with the Registry defined below and will be periodically sent to
// the ganglia backend. Each metric defines a writeMeta and a writeValue method
// as defined below and is responsible for writing it's data to the given
// gmetric.Client.
type metric interface {
	name() string
	writeMeta(c *gmetric.Client)
	writeValue(c *gmetric.Client)
	register(r *Registry)
}

// Registry provides the process to periodically report the in-memory metrics
// to Ganglia.
type Registry struct {
	Prefix            string
	NameSeparator     string          // Default is a dot "."
	Client            *gmetric.Client `inject:""`
	WriteTickDuration time.Duration
	startOnce         sync.Once
	metrics           []metric
	mutex             sync.Mutex
}

func (r *Registry) start() {
	go func() {
		sendTicker := time.NewTicker(r.WriteTickDuration)
		metricsTicker := time.NewTicker(metrics.TickDuration)
		for {
			select {
			case <-sendTicker.C:
				r.write()
			case <-metricsTicker.C:
				r.tick()
			}
		}
	}()
}

func (r *Registry) write() {
	ms := r.registered()
	for _, m := range ms {
		m.writeMeta(r.Client)
		m.writeValue(r.Client)
	}
}

func (r *Registry) tick() {
	ms := r.registered()
	for _, m := range ms {
		if t, ok := m.(metrics.Tickable); ok {
			t.Tick()
		}
	}
}

// Register a metric. The only metrics acceptable for registration are the ones
// provided in this package itself. The registration function uses an untyped
// argument to make it easier for use with fields typed as one of the metrics
// in the go.metrics library. All the metrics provided by this library embed
// one of those metrics and augment them with Ganglia specific metadata.
func (r *Registry) Register(m interface{}) {
	r.startOnce.Do(r.start)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	v, ok := m.(metric)
	if !ok {
		panic(fmt.Sprintf("unknown metric type: %T", m))
	}
	v.register(r)
	r.metrics = append(r.metrics, v)
}

// Get the metric with the given name.
func (r *Registry) Get(name string) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, m := range r.metrics {
		if m.name() == name {
			return m
		}
	}
	return nil
}

func (r *Registry) registered() []metric {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	metrics := make([]metric, len(r.metrics))
	copy(metrics, r.metrics)
	return metrics
}

func (r *Registry) makeName(parts ...string) string {
	var nonempty []string
	if r.Prefix != "" {
		nonempty = append(nonempty, r.Prefix)
	}
	for _, p := range parts {
		if p != "" {
			nonempty = append(nonempty, p)
		}
	}
	return strings.Join(nonempty, r.nameSeparator())
}

func (r *Registry) nameSeparator() string {
	if r.NameSeparator == "" {
		return "."
	}
	return r.NameSeparator
}

// NewTestRegistry returns a Registry that does not automatically tick or write
// metrics (and hence doesn't need a running client or server).
func NewTestRegistry() *Registry {
	r := &Registry{}
	// consume the start once to disable the background goroutine
	r.startOnce.Do(func() {})
	return r
}
