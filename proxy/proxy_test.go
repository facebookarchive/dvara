package proxy

import (
	"fmt"
	"strings"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/mgotest"
	"github.com/facebookgo/startstop"
	"github.com/facebookgo/stats"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func TestParallelInsertWithUniqueIndex(t *testing.T) {
	t.Parallel()
	if disableSlowTests {
		t.Skip("TestParallelInsertWithUniqueIndex disabled because it's slow")
	}
	h := NewSingleHarness(t)
	defer h.Stop()

	limit := 20000
	c := make(chan int, limit)
	for i := 0; i < 3; i++ {
		go inserter(h.ProxySession(), c, limit)
	}
	set := make(map[int]bool)
	for k := range c {
		if set[k] {
			t.Fatal("Double write on same value")
		}
		set[k] = true
		if len(set) == limit {
			break
		}
	}
}

func inserter(s *mgo.Session, channel chan int, limit int) {
	defer s.Close()
	c := s.DB("test").C("test")
	c.EnsureIndex(mgo.Index{Key: []string{"phoneNum"}, Unique: true})
	for i := 1; i <= limit; i++ {
		if err := c.Insert(bson.M{"phoneNum": i}); err == nil {
			channel <- i
		}
	}
}
func TestSimpleCRUD(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	defer p.Stop()
	session := p.ProxySession()
	defer session.Close()
	collection := session.DB("test").C("coll1")
	data := map[string]interface{}{
		"_id":  1,
		"name": "abc",
	}
	err := collection.Insert(data)
	if err != nil {
		t.Fatal("insertion error", err)
	}
	n, err := collection.Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expecting 1 got %d", n)
	}
	result := make(map[string]interface{})
	collection.Find(bson.M{"_id": 1}).One(&result)
	if result["name"] != "abc" {
		t.Fatal("expecting name abc got", result)
	}
	err = collection.DropCollection()
	if err != nil {
		t.Fatal(err)
	}
}

// inserting data with same id field twice should fail
func TestIDConstraint(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	defer p.Stop()
	session := p.ProxySession()
	defer session.Close()
	collection := session.DB("test").C("coll1")
	data := map[string]interface{}{
		"_id":  1,
		"name": "abc",
	}
	err := collection.Insert(data)
	if err != nil {
		t.Fatal("insertion error", err)
	}
	err = collection.Insert(data)
	if err == nil {
		t.Fatal("insertion failed on same id without write concern")
	}
}

// inserting data voilating index clause on a separate connection should fail
func TestEnsureIndex(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	defer p.Stop()
	session := p.ProxySession()
	collection := session.DB("test").C("coll1")
	index := mgo.Index{
		Key:        []string{"lastname", "firstname"},
		Unique:     true,
		DropDups:   true,
		Background: true, // See notes.
		Sparse:     true,
	}
	err := collection.EnsureIndex(index)
	ensure.Nil(t, err)
	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	if err != nil {
		t.Fatal("insertion error", err)
	}
	session.Close()
	session = p.ProxySession()
	defer session.Close()
	collection = session.DB("test").C("coll1")
	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	ensure.NotNil(t, err)
}

// inserting same data after dropping an index should work
func TestDropIndex(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	defer p.Stop()
	session := p.ProxySession()
	collection := session.DB("test").C("coll1")
	index := mgo.Index{
		Key:        []string{"lastname", "firstname"},
		Unique:     true,
		DropDups:   true,
		Background: true, // See notes.
		Sparse:     true,
	}
	err := collection.EnsureIndex(index)
	if err != nil {
		t.Fatal("ensure index call failed")
	}
	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	if err != nil {
		t.Fatal("insertion error", err)
	}
	collection.DropIndex("lastname", "firstname")
	session.Close()
	session = p.ProxySession()
	defer session.Close()
	collection = session.DB("test").C("coll1")
	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	if err != nil {
		t.Fatal("drop index did not work")
	}
}

func TestRemoval(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	defer p.Stop()
	session := p.ProxySession()
	defer session.Close()
	collection := session.DB("test").C("coll1")
	if err := collection.Insert(bson.M{"S": "hello", "I": 24}); err != nil {
		t.Fatal(err)
	}
	if err := collection.Remove(bson.M{"S": "hello", "I": 24}); err != nil {
		t.Fatal(err)
	}
	var res []interface{}
	collection.Find(bson.M{"S": "hello", "I": 24}).All(&res)
	if res != nil {
		t.Fatal("found object after delete", res)
	}
	if err := collection.Remove(bson.M{"S": "hello", "I": 24}); err == nil {
		t.Fatal("removing nonexistant document should error")
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	defer p.Stop()
	session := p.ProxySession()
	defer session.Close()
	collection := session.DB("test").C("coll1")
	if err := collection.Insert(bson.M{"_id": "1234", "name": "Alfred"}); err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	collection.Find(nil).One(&result)
	if result["name"] != "Alfred" {
		t.Fatal("insert failed")
	}
	if err := collection.Update(bson.M{"_id": "1234"}, bson.M{"name": "Jeeves"}); err != nil {
		t.Fatal("update failed with", err)
	}
	collection.Find(nil).One(&result)
	if result["name"] != "Jeeves" {
		t.Fatal("update failed")
	}
	if err := collection.Update(bson.M{"_id": "00000"}, bson.M{"name": "Jeeves"}); err == nil {
		t.Fatal("update failed")
	}
}

func TestStopChattyClient(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	session := p.ProxySession()
	defer session.Close()
	fin := make(chan struct{})
	go func() {
		collection := session.DB("test").C("coll1")
		i := 0
		for {
			select {
			default:
				collection.Insert(bson.M{"value": i})
				i++
			case <-fin:
				return
			}
		}
	}()
	close(fin)
	p.Stop()
}

func TestStopIdleClient(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	session := p.ProxySession()
	defer session.Close()
	if err := session.DB("test").C("col").Insert(bson.M{"v": 1}); err != nil {
		t.Fatal(err)
	}
	p.Stop()
}

func TestZeroMaxConnections(t *testing.T) {
	t.Parallel()
	p := &Proxy{ReplicaSet: &ReplicaSet{}}
	err := p.Start()
	if err != errZeroMaxConnections {
		t.Fatal("did not get expected error")
	}
}

func TestNoAddrsGiven(t *testing.T) {
	t.Parallel()
	replicaSet := ReplicaSet{MaxConnections: 1}
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
	err = startstop.Start(objects, &log)
	if err != errNoAddrsGiven {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestSingleNodeWhenExpectingRS(t *testing.T) {
	t.Parallel()
	mgoserver := mgotest.NewStartedServer(t)
	defer mgoserver.Stop()
	replicaSet := ReplicaSet{
		Addrs:          fmt.Sprintf("127.0.0.1:%d,127.0.0.1:%d", mgoserver.Port, mgoserver.Port+1),
		MaxConnections: 1,
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
	err = startstop.Start(objects, &log)
	if err == nil || !strings.Contains(err.Error(), "was expecting it to be in a replica set") {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestStopListenerCloseError(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	p.Stop()
	err := p.ReplicaSet.Stop()
	if err == nil || !strings.Contains(err.Error(), "use of closed network connection") {
		t.Fatalf("did not get expected error, instead got: %s", err)
	}
}

func TestMongoGoingAwayAndReturning(t *testing.T) {
	t.Parallel()
	p := NewSingleHarness(t)
	session := p.ProxySession()
	defer session.Close()
	collection := session.DB("test").C("coll1")
	if err := collection.Insert(bson.M{"value": 1}); err != nil {
		t.Fatal(err)
	}
	p.MgoServer.Stop()
	p.MgoServer.Start()
	// For now we can only gurantee that eventually things will work again. In an
	// ideal world the very first client connection after mongo returns should
	// work, and we shouldn't need a loop here.
	for {
		collection = session.Copy().DB("test").C("coll1")
		if err := collection.Insert(bson.M{"value": 3}); err == nil {
			break
		}
	}
	p.Stop()
}

func benchmarkInsertRead(b *testing.B, session *mgo.Session) {
	defer session.Close()
	col := session.DB("test").C("col")
	col.EnsureIndex(mgo.Index{Key: []string{"answer"}, Unique: true})
	insertDocs := bson.D{bson.DocElem{Name: "answer"}}
	inserted := bson.M{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		insertDocs[0].Value = i
		if err := col.Insert(insertDocs); err != nil {
			b.Fatal(err)
		}
		if err := col.Find(insertDocs).One(inserted); err != nil {
			b.Fatal(err)
		}
		if _, ok := inserted["_id"]; !ok {
			b.Fatalf("no _id found: %+v", inserted)
		}
	}
}

func BenchmarkInsertReadProxy(b *testing.B) {
	p := NewSingleHarness(b)
	benchmarkInsertRead(b, p.ProxySession())
}

func BenchmarkInsertReadDirect(b *testing.B) {
	p := NewSingleHarness(b)
	benchmarkInsertRead(b, p.RealSession())
}
