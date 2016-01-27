package dvara

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/facebookgo/ensure"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"

	"gopkg.in/mgo.v2/bson"
)

var errInvalidBSON = errors.New("invalid BSON")

type invalidBSON int

func (i invalidBSON) GetBSON() (interface{}, error) {
	return nil, errInvalidBSON
}

var errProxyNotFound = errors.New("proxy not found")

type fakeProxyMapper struct {
	m map[string]string
}

func (t fakeProxyMapper) Proxy(h string) (string, error) {
	if t.m != nil {
		if r, ok := t.m[h]; ok {
			return r, nil
		}
	}
	return "", errProxyNotFound
}

type fakeReplicaStateCompare struct{ sameRS, sameIM bool }

func (f fakeReplicaStateCompare) SameRS(o *replSetGetStatusResponse) bool {
	return f.sameRS
}

func (f fakeReplicaStateCompare) SameIM(o *isMasterResponse) bool {
	return f.sameIM
}

func fakeReader(h messageHeader, rest []byte) io.Reader {
	return bytes.NewReader(append(h.ToWire(), rest...))
}

func fakeSingleDocReply(v interface{}) io.Reader {
	b, err := bson.Marshal(v)
	if err != nil {
		panic(err)
	}
	b = append(
		[]byte{
			0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0,
			1, 0, 0, 0,
		},
		b...,
	)
	h := messageHeader{
		OpCode:        OpReply,
		MessageLength: int32(headerLen + len(b)),
	}
	return fakeReader(h, b)
}

type fakeReadWriter struct {
	io.Reader
	io.Writer
}

func TestResponseRWReadOne(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name   string
		Server io.Reader
		Error  string
	}{
		{
			Name:   "no header",
			Server: bytes.NewReader(nil),
			Error:  "EOF",
		},
		{
			Name:   "non reply op",
			Server: bytes.NewReader((messageHeader{OpCode: OpDelete}).ToWire()),
			Error:  "expected op REPLY, got DELETE",
		},
		{
			Name:   "EOF before flags",
			Server: bytes.NewReader((messageHeader{OpCode: OpReply}).ToWire()),
			Error:  "EOF",
		},
		{
			Name: "more than 1 document",
			Server: fakeReader(
				messageHeader{OpCode: OpReply},
				[]byte{
					0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0,
					2, 0, 0, 0,
				},
			),
			Error: "can only handle 1 result document, got: 2",
		},
		{
			Name: "EOF before document",
			Server: fakeReader(
				messageHeader{OpCode: OpReply},
				[]byte{
					0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0,
					1, 0, 0, 0,
				},
			),
			Error: "EOF",
		},
		{
			Name: "corrupted document",
			Server: fakeReader(
				messageHeader{OpCode: OpReply},
				[]byte{
					0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0,
					1, 0, 0, 0,
					5, 0, 0, 0,
					1,
				},
			),
			Error: "Document is corrupted",
		},
	}

	for _, c := range cases {
		r := &ReplyRW{Log: &tLogger{TB: t}}
		m := bson.M{}
		_, _, _, err := r.ReadOne(c.Server, m)
		if err == nil {
			t.Errorf("was expecting an error for case %s", c.Name)
		}
		if !strings.Contains(err.Error(), c.Error) {
			t.Errorf("did not get expected error for case %s instead got %s", c.Name, err)
		}
	}
}

func TestResponseRWWriteOne(t *testing.T) {
	errWrite := errors.New("write error")
	t.Parallel()
	cases := []struct {
		Name   string
		Client io.Writer
		Header messageHeader
		Prefix replyPrefix
		DocLen int32
		Value  interface{}
		Error  string
	}{
		{
			Name:  "invalid bson",
			Value: invalidBSON(0),
			Error: errInvalidBSON.Error(),
		},
		{
			Name:  "write error",
			Value: map[string]string{},
			Client: testWriter{
				write: func(b []byte) (int, error) {
					return 0, errWrite
				},
			},
			Error: errWrite.Error(),
		},
	}

	for _, c := range cases {
		r := &ReplyRW{Log: &tLogger{TB: t}}
		err := r.WriteOne(c.Client, &c.Header, c.Prefix, c.DocLen, c.Value)
		if err == nil {
			t.Errorf("was expecting an error for case %s", c.Name)
		}
		if !strings.Contains(err.Error(), c.Error) {
			t.Errorf("did not get expected error for case %s instead got %s", c.Name, err)
		}
	}
}

func TestIsMasterResponseRewriterFailures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name                string
		Client              io.Writer
		Server              io.Reader
		ProxyMapper         ProxyMapper
		ReplicaStateCompare ReplicaStateCompare
		Error               string
	}{
		{
			Name:   "no header",
			Server: bytes.NewReader(nil),
			Error:  "EOF",
		},
		{
			Name: "unknown host in 'hosts'",
			Server: fakeSingleDocReply(
				map[string]interface{}{
					"hosts": []string{"foo"},
				},
			),
			Error:               errProxyNotFound.Error(),
			ProxyMapper:         fakeProxyMapper{},
			ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		},
		{
			Name: "unknown host in 'primary'",
			Server: fakeSingleDocReply(
				map[string]interface{}{
					"primary": "foo",
				},
			),
			Error:               errProxyNotFound.Error(),
			ProxyMapper:         fakeProxyMapper{},
			ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		},
		{
			Name: "unknown host in 'me'",
			Server: fakeSingleDocReply(
				map[string]interface{}{
					"me": "foo",
				},
			),
			Error:               errProxyNotFound.Error(),
			ProxyMapper:         fakeProxyMapper{},
			ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		},
		{
			Name:                "different im",
			Server:              fakeSingleDocReply(map[string]interface{}{}),
			Error:               errRSChanged.Error(),
			ProxyMapper:         nil,
			ReplicaStateCompare: fakeReplicaStateCompare{sameIM: false, sameRS: true},
		},
	}

	for _, c := range cases {
		r := &IsMasterResponseRewriter{
			Log:                 &tLogger{TB: t},
			ProxyMapper:         c.ProxyMapper,
			ReplicaStateCompare: c.ReplicaStateCompare,
			ReplyRW: &ReplyRW{
				Log: &tLogger{TB: t},
			},
		}
		err := r.Rewrite(c.Client, c.Server)
		if err == nil {
			t.Errorf("was expecting an error for case %s", c.Name)
		}
		if !strings.Contains(err.Error(), c.Error) {
			t.Errorf("did not get expected error for case %s instead got %s", c.Name, err)
		}
	}
}

func TestIsMasterResponseRewriterSuccess(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"hosts":   []interface{}{"a", "b", "c"},
		"me":      "a",
		"primary": "b",
		"foo":     "bar",
	}
	out := bson.M{
		"hosts":   []interface{}{"1", "2", "3"},
		"me":      "1",
		"primary": "2",
		"foo":     "bar",
	}

	r := &IsMasterResponseRewriter{
		Log:                 &tLogger{TB: t},
		ProxyMapper:         proxyMapper,
		ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		ReplyRW: &ReplyRW{
			Log: &tLogger{TB: t},
		},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err != nil {
		t.Fatal(err)
	}
	actualOut := bson.M{}
	doc := client.Bytes()[headerLen+len(emptyPrefix):]
	if err := bson.Unmarshal(doc, &actualOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, actualOut) {
		spew.Dump(out)
		spew.Dump(actualOut)
		t.Fatal("did not get expected output")
	}
}

func TestIsMasterResponseRewriterSuccessWithPassives(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"hosts":    []interface{}{"a", "b", "c"},
		"me":       "a",
		"primary":  "b",
		"foo":      "bar",
		"passives": []interface{}{"a"},
	}
	out := bson.M{
		"hosts":    []interface{}{"1", "2", "3"},
		"me":       "1",
		"primary":  "2",
		"foo":      "bar",
		"passives": []interface{}{"1"},
	}

	r := &IsMasterResponseRewriter{
		Log:                 &tLogger{TB: t},
		ProxyMapper:         proxyMapper,
		ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		ReplyRW: &ReplyRW{
			Log: &tLogger{TB: t},
		},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err != nil {
		t.Fatal(err)
	}
	actualOut := bson.M{}
	doc := client.Bytes()[headerLen+len(emptyPrefix):]
	if err := bson.Unmarshal(doc, &actualOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, actualOut) {
		spew.Dump(out)
		spew.Dump(actualOut)
		t.Fatal("did not get expected output")
	}
}

func TestReplSetGetStatusResponseRewriterFailures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name                string
		Client              io.Writer
		Server              io.Reader
		ProxyMapper         ProxyMapper
		ReplicaStateCompare ReplicaStateCompare
		Error               string
	}{
		{
			Name:   "no header",
			Server: bytes.NewReader(nil),
			Error:  "EOF",
		},
		{
			Name: "unknown member name",
			Server: fakeSingleDocReply(
				map[string]interface{}{
					"members": []map[string]interface{}{
						{
							"name": "foo",
						},
					},
				},
			),
			Error:               errProxyNotFound.Error(),
			ProxyMapper:         fakeProxyMapper{},
			ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		},
		{
			Name:                "diffferent rs",
			Server:              fakeSingleDocReply(map[string]interface{}{}),
			Error:               errRSChanged.Error(),
			ProxyMapper:         nil,
			ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: false},
		},
	}

	for _, c := range cases {
		r := &ReplSetGetStatusResponseRewriter{
			Log:                 &tLogger{TB: t},
			ProxyMapper:         c.ProxyMapper,
			ReplicaStateCompare: c.ReplicaStateCompare,
			ReplyRW: &ReplyRW{
				Log: &tLogger{TB: t},
			},
		}
		err := r.Rewrite(c.Client, c.Server)
		if err == nil {
			t.Errorf("was expecting an error for case %s", c.Name)
		}
		if !strings.Contains(err.Error(), c.Error) {
			t.Errorf("did not get expected error for case %s instead got %s", c.Name, err)
		}
	}
}

func TestReplSetGetStatusResponseRewriterSuccess(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"members": []interface{}{
			bson.M{
				"name":     "a",
				"stateStr": "PRIMARY",
			},
			bson.M{
				"name": "b",
			},
			bson.M{
				"name":     "c",
				"stateStr": "ARBITER",
			},
		},
	}
	out := bson.M{
		"members": []interface{}{
			bson.M{
				"name":     "1",
				"stateStr": "PRIMARY",
			},
			bson.M{
				"name": "2",
			},
			bson.M{
				"name":     "3",
				"stateStr": "ARBITER",
			},
		},
	}
	r := &ReplSetGetStatusResponseRewriter{
		Log:                 &tLogger{TB: t},
		ProxyMapper:         proxyMapper,
		ReplicaStateCompare: fakeReplicaStateCompare{sameIM: true, sameRS: true},
		ReplyRW: &ReplyRW{
			Log: &tLogger{TB: t},
		},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err != nil {
		t.Fatal(err)
	}
	actualOut := bson.M{}
	doc := client.Bytes()[headerLen+len(emptyPrefix):]
	if err := bson.Unmarshal(doc, &actualOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, actualOut) {
		spew.Dump(out)
		spew.Dump(actualOut)
		t.Fatal("did not get expected output")
	}
}

func TestProxyQuery(t *testing.T) {
	t.Parallel()
	var p ProxyQuery
	log := tLogger{TB: t}
	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &fakeProxyMapper{}},
		&inject.Object{Value: &fakeReplicaStateCompare{}},
		&inject.Object{Value: &log},
		&inject.Object{Value: &p},
	)
	ensure.Nil(t, err)
	ensure.Nil(t, graph.Populate())
	objects := graph.Objects()
	ensure.Nil(t, startstop.Start(objects, &log))
	defer startstop.Stop(objects, &log)

	cases := []struct {
		Name   string
		Header *messageHeader
		Client io.ReadWriter
		Error  string
	}{
		{
			Name:   "EOF while reading flags from client",
			Header: &messageHeader{},
			Client: new(bytes.Buffer),
			Error:  "EOF",
		},
		{
			Name:   "EOF while reading collection name",
			Header: &messageHeader{},
			Client: fakeReadWriter{
				Reader: bytes.NewReader(
					[]byte{0, 0, 0, 0}, // flags int32 before collection name
				),
			},
			Error: "EOF",
		},
		{
			Name:   "EOF while reading skip/return",
			Header: &messageHeader{},
			Client: fakeReadWriter{
				Reader: bytes.NewReader(
					append(
						[]byte{0, 0, 0, 0}, // flags int32 before collection name
						adminCollectionName...,
					),
				),
			},
			Error: "EOF",
		},
		{
			Name:   "EOF while reading query document",
			Header: &messageHeader{},
			Client: fakeReadWriter{
				Reader: io.MultiReader(
					bytes.NewReader([]byte{0, 0, 0, 0}), // flags int32 before collection name
					bytes.NewReader(adminCollectionName),
					bytes.NewReader(
						[]byte{
							0, 0, 0, 0, // numberToSkip int32
							0, 0, 0, 0, // numberToReturn int32
							1, // partial bson document length header
						}),
				),
			},
			Error: "EOF",
		},
		{
			Name:   "error while unmarshaling query document",
			Header: &messageHeader{},
			Client: fakeReadWriter{
				Reader: io.MultiReader(
					bytes.NewReader([]byte{0, 0, 0, 0}), // flags int32 before collection name
					bytes.NewReader(adminCollectionName),
					bytes.NewReader(
						[]byte{
							0, 0, 0, 0, // numberToSkip int32
							0, 0, 0, 0, // numberToReturn int32
							5, 0, 0, 0, // bson document length header
							1, // bson document
						}),
				),
			},
			Error: "Document is corrupted",
		},
	}

	for _, c := range cases {
		err := p.Proxy(c.Header, c.Client, nil, nil)
		if err == nil || !strings.Contains(err.Error(), c.Error) {
			t.Fatalf("did not find expected error for %s, instead found %s", c.Name, err)
		}
	}
}
