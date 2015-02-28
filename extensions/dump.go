package extensions

import (
	"fmt"
	"io"

	"github.com/mcuadros/exmongodb/protocol"
)

type DumpExtension struct{}

func (e *DumpExtension) Handle(
	header *protocol.MessageHeader,
	client io.ReadWriter,
	server io.ReadWriter,
	lastError *protocol.LastError,
) (cont bool, err error) {
	fmt.Println("DUMP", header)

	return true, nil
}
