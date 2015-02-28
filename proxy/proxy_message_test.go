package proxy

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/mcuadros/exmongodb/protocol"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/gangliamr"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
)

func TestProxyMessage(t *testing.T) {
	t.Parallel()
	var p ProxyMessage
	var log nopLogger
	var ext mockExtension
	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &fakeProxyMapper{}},
		&inject.Object{Value: &fakeReplicaStateCompare{}},
		&inject.Object{Value: &log},
		&inject.Object{Value: &ext},
		&inject.Object{Value: &p},
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
	defer startstop.Stop(objects, &log)

	cases := []struct {
		Name   string
		Header *protocol.MessageHeader
		Client io.ReadWriter
		Error  string
	}{
		{
			Name:   "EOF while reading flags from client",
			Header: &protocol.MessageHeader{},
			Client: new(bytes.Buffer),
			Error:  "EOF",
		},
		{
			Name:   "EOF while reading collection name",
			Header: &protocol.MessageHeader{},
			Client: fakeReadWriter{
				Reader: bytes.NewReader(
					[]byte{0, 0, 0, 0}, // flags int32 before collection name
				),
			},
			Error: "EOF",
		},
		{
			Name:   "EOF while reading skip/return",
			Header: &protocol.MessageHeader{},
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
			Header: &protocol.MessageHeader{},
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
			Header: &protocol.MessageHeader{},
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
		err := p.proxyQuery(c.Header, c.Client, nil, nil)
		if err == nil || !strings.Contains(err.Error(), c.Error) {
			t.Fatalf("did not find expected error for %s, instead found %s", c.Name, err)
		}
	}
}
