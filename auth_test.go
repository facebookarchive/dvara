package dvara

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestSimpleCRUDWithAuthEnabled(t *testing.T) {
	t.Parallel()
	p := NewSingleHarnessWithAuthEnabled(t)
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
