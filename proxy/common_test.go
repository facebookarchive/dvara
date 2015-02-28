package proxy

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/mcuadros/exmongodb/protocol"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/gangliamr"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/mgotest"
	"github.com/facebookgo/startstop"
	"github.com/facebookgo/stats"
	"gopkg.in/mgo.v2"
)

var disableSlowTests = os.Getenv("GO_RUN_LONG_TEST") == ""

type nopLogger struct{}

func (n nopLogger) Error(args ...interface{})                 {}
func (n nopLogger) Errorf(format string, args ...interface{}) {}
func (n nopLogger) Warn(args ...interface{})                  {}
func (n nopLogger) Warnf(format string, args ...interface{})  {}
func (n nopLogger) Info(args ...interface{})                  {}
func (n nopLogger) Infof(format string, args ...interface{})  {}
func (n nopLogger) Debug(args ...interface{})                 {}
func (n nopLogger) Debugf(format string, args ...interface{}) {}

type stopper interface {
	Stop()
}

type Harness struct {
	T          testing.TB
	Stopper    stopper // This is either mgotest.Server or mgotest.ReplicaSet
	ReplicaSet *ReplicaSet
	Graph      *inject.Graph
	Log        nopLogger
}

func newHarnessInternal(url string, s stopper, t testing.TB) *Harness {
	replicaSet := ReplicaSet{
		Addrs:                   url,
		PortStart:               2000,
		PortEnd:                 3000,
		MaxConnections:          5,
		MinIdleConnections:      5,
		ServerIdleTimeout:       5 * time.Minute,
		ServerClosePoolSize:     5,
		ClientIdleTimeout:       5 * time.Minute,
		MaxPerClientConnections: 250,
		GetLastErrorTimeout:     5 * time.Minute,
		MessageTimeout:          5 * time.Second,
	}
	var log nopLogger
	var graph inject.Graph
	var ext mockExtension
	err := graph.Provide(
		&inject.Object{Value: &log},
		&inject.Object{Value: &replicaSet},
		&inject.Object{Value: &stats.HookClient{}},
		&inject.Object{Value: &ext},
	)
	ensure.Nil(t, err)
	ensure.Nil(t, graph.Populate())
	objects := graph.Objects()
	gregistry := gangliamr.NewTestRegistry()
	for _, o := range objects {
		if rmO, ok := o.Value.(registerMetrics); ok {
			rmO.RegisterMetrics(gregistry)
		}
	}
	ensure.Nil(t, startstop.Start(objects, &log))
	return &Harness{
		T:          t,
		Stopper:    s,
		ReplicaSet: &replicaSet,
		Graph:      &graph,
	}
}

type SingleHarness struct {
	*Harness
	MgoServer *mgotest.Server
}

func NewSingleHarness(t testing.TB) *SingleHarness {
	mgoserver := mgotest.NewStartedServer(t)
	return &SingleHarness{
		Harness:   newHarnessInternal(mgoserver.URL(), mgoserver, t),
		MgoServer: mgoserver,
	}
}

type ReplicaSetHarness struct {
	*Harness
	MgoReplicaSet *mgotest.ReplicaSet
}

func NewReplicaSetHarness(n uint, t testing.TB) *ReplicaSetHarness {
	if disableSlowTests {
		t.Skip("disabled because it's slow")
	}
	mgoRS := mgotest.NewReplicaSet(n, t)
	return &ReplicaSetHarness{
		Harness:       newHarnessInternal(mgoRS.Addrs()[n-1], mgoRS, t),
		MgoReplicaSet: mgoRS,
	}
}

func (h *Harness) Stop() {
	defer h.Stopper.Stop()
	ensure.Nil(h.T, startstop.Stop(h.Graph.Objects(), &h.Log))
}

func (h *Harness) ProxySession() *mgo.Session {
	return h.Dial(h.ReplicaSet.ProxyMembers()[0])
}

func (h *Harness) RealSession() *mgo.Session {
	return h.Dial(h.ReplicaSet.lastState.Addrs()[0])
}

func (h *Harness) Dial(u string) *mgo.Session {
	session, err := mgo.Dial(u)
	ensure.Nil(h.T, err, u)
	session.SetSafe(&mgo.Safe{FSync: true, W: 1})
	session.SetSyncTimeout(time.Minute)
	session.SetSocketTimeout(time.Minute)
	return session
}

type registerMetrics interface {
	RegisterMetrics(r *gangliamr.Registry)
}

type mockExtension struct{}

func (e *mockExtension) Handle(
	header *protocol.MessageHeader,
	client io.ReadWriter,
	server io.ReadWriter,
	lastError *protocol.LastError,
) (cont bool, err error) {
	return true, nil
}
