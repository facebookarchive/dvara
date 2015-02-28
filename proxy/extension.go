package proxy

import (
	"io"

	"github.com/mcuadros/exmongodb/protocol"
)

type Extension interface {
	Handle(
		header *protocol.MessageHeader,
		client io.ReadWriter,
		server io.ReadWriter,
		lastError *protocol.LastError,
	) (cont bool, err error)
}
