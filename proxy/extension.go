package proxy

import (
	"io"

	"github.com/mcuadros/exmongodb/protocol"

	"gopkg.in/mgo.v2/bson"
)

type Extension interface {
	HandleOp(
		header *protocol.MessageHeader,
		client io.ReadWriter,
		server io.ReadWriter,
		lastError *protocol.LastError,
	) (cont bool, err error)

	HandleBSON(
		bson *bson.D,
		client io.ReadWriter,
		server io.ReadWriter,
		lastError *protocol.LastError,
	) (cont bool, err error)
}
