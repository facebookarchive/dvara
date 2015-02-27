package extensions

import (
	"fmt"
	"net"

	"github.com/mcuadros/exmongodb/protocol"
)

type DumpExtension struct{}

func (e *DumpExtension) Handle(
	header *protocol.MessageHeader,
	client net.Conn,
	server net.Conn,
	lastError *protocol.LastError,
) (cont bool, err error) {
	fmt.Println(header)

	return true, nil
}
