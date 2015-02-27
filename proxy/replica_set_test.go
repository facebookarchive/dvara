package proxy

import (
	"fmt"
	"testing"

	"github.com/facebookgo/subset"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func TestReplicaSetMembers(t *testing.T) {
	t.Parallel()
	h := NewReplicaSetHarness(3, t)
	defer h.Stop()

	proxyMembers := h.ReplicaSet.ProxyMembers()
	session := h.ProxySession()
	defer session.Close()
	status, err := replSetGetStatus(session)
	if err != nil {
		t.Fatal(err)
	}

outerProxyResponseCheckLoop:
	for _, m := range status.Members {
		for _, p := range proxyMembers {
			if m.Name == p {
				continue outerProxyResponseCheckLoop
			}
		}
		t.Fatalf("Unexpected member: %s", m.Name)
	}
}

func TestStopNodeInReplica(t *testing.T) {
	t.Parallel()
	h := NewReplicaSetHarness(2, t)
	defer h.Stop()

	const dbName = "test"
	const colName = "foo"
	const keyName = "answer"
	d := bson.M{"answer": "42"}
	s := h.ProxySession()
	defer s.Close()
	s.SetSafe(&mgo.Safe{W: 2, WMode: "majority"})
	if err := s.DB(dbName).C(colName).Insert(d); err != nil {
		t.Fatal(err)
	}

	h.MgoReplicaSet.Servers[0].Stop()

	s.SetMode(mgo.Monotonic, true)
	var actual bson.M
	if err := s.DB(dbName).C(colName).Find(d).One(&actual); err != nil {
		t.Fatal(err)
	}

	subset.Assert(t, d, actual)
}

func TestProxyNotInReplicaSet(t *testing.T) {
	t.Parallel()
	h := NewSingleHarness(t)
	defer h.Stop()
	addr := "127.0.0.1:666"
	expected := fmt.Sprintf("mongo %s is not in ReplicaSet", addr)
	_, err := h.ReplicaSet.Proxy(addr)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddSameProxyToReplicaSet(t *testing.T) {
	t.Parallel()
	r := &ReplicaSet{
		Log:         nopLogger{},
		proxyToReal: make(map[string]string),
		realToProxy: make(map[string]string),
		proxies:     make(map[string]*Proxy),
	}
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if err := r.add(p); err != nil {
		t.Fatal(err)
	}
	expected := fmt.Sprintf("proxy %s already used in ReplicaSet", p.ProxyAddr)
	err := r.add(p)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddSameMongoToReplicaSet(t *testing.T) {
	t.Parallel()
	r := &ReplicaSet{
		Log:         nopLogger{},
		proxyToReal: make(map[string]string),
		realToProxy: make(map[string]string),
		proxies:     make(map[string]*Proxy),
	}
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if err := r.add(p); err != nil {
		t.Fatal(err)
	}
	p = &Proxy{
		ProxyAddr: "3",
		MongoAddr: p.MongoAddr,
	}
	expected := fmt.Sprintf("mongo %s already exists in ReplicaSet", p.MongoAddr)
	err := r.add(p)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestNewListenerZeroZeroRandomPort(t *testing.T) {
	t.Parallel()
	r := &ReplicaSet{}
	l, err := r.newListener()
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}

func TestNewListenerError(t *testing.T) {
	t.Parallel()
	r := &ReplicaSet{PortStart: 1, PortEnd: 1}
	_, err := r.newListener()
	expected := "could not find a free port in range 1-1"
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}
