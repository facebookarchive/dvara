package proxy

import (
	"net"

	"github.com/mcuadros/exmongodb/protocol"
)

type Extension interface {
	Handle(
		header *protocol.MessageHeader,
		client net.Conn,
		server net.Conn,
		lastError *protocol.LastError,
	) (cont bool, err error)
}
