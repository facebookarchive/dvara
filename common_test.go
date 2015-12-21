package dvara

import (
	"os"
	"testing"
	"time"

	"gopkg.in/mgo.v2"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/mgotest"
	"github.com/facebookgo/startstop"
	"github.com/facebookgo/stats"
)

var (
	disableSlowTests = os.Getenv("GO_RUN_LONG_TEST") == ""
	veryVerbose      = os.Getenv("VERY_VERBOSE") == "1"
)

type tLogger struct {
	TB testing.TB
}

func (l *tLogger) Error(args ...interface{}) {
	if veryVerbose {
		l.TB.Log(args...)
	}
}

func (l *tLogger) Errorf(format string, args ...interface{}) {
	if veryVerbose {
		l.TB.Logf(format, args...)
	}
}

func (l *tLogger) Warn(args ...interface{}) {
	if veryVerbose {
		l.TB.Log(args...)
	}
}

func (l *tLogger) Warnf(format string, args ...interface{}) {
	if veryVerbose {
		l.TB.Logf(format, args...)
	}
}

func (l *tLogger) Info(args ...interface{}) {
	if veryVerbose {
		l.TB.Log(args...)
	}
}

func (l *tLogger) Infof(format string, args ...interface{}) {
	if veryVerbose {
		l.TB.Logf(format, args...)
	}
}

func (l *tLogger) Debug(args ...interface{}) {
	if veryVerbose {
		l.TB.Log(args...)
	}
}

func (l *tLogger) Debugf(format string, args ...interface{}) {
	if veryVerbose {
		l.TB.Logf(format, args...)
	}
}

type stopper interface {
	Stop()
}

type Harness struct {
	T          testing.TB
	Stopper    stopper // This is either mgotest.Server or mgotest.ReplicaSet
	ReplicaSet *ReplicaSet
	Graph      *inject.Graph
	Log        *tLogger
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
		MessageTimeout:          time.Minute,
	}
	log := tLogger{TB: t}
	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &log},
		&inject.Object{Value: &replicaSet},
		&inject.Object{Value: &stats.HookClient{}},
	)
	ensure.Nil(t, err)
	ensure.Nil(t, graph.Populate())
	objects := graph.Objects()
	ensure.Nil(t, startstop.Start(objects, &log))
	return &Harness{
		T:          t,
		Stopper:    s,
		ReplicaSet: &replicaSet,
		Graph:      &graph,
		Log:        &log,
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

func NewSingleHarnessWithAuthEnabled(t testing.TB) *SingleHarness {
	mgoserver := mgotest.NewStartedServer(t, "--auth")
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
	ensure.Nil(h.T, startstop.Stop(h.Graph.Objects(), h.Log))
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
