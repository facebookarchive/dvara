package extensions

import (
	"fmt"
	"io"

	"github.com/mcuadros/exmongodb/protocol"

	"gopkg.in/mgo.v2/bson"
)

type DumpExtension struct{}

func (e *DumpExtension) HandleOp(
	header *protocol.MessageHeader,
	client io.ReadWriter,
	server io.ReadWriter,
	lastError *protocol.LastError,
) (cont bool, err error) {
	fmt.Println("DUMP", header)

	return true, nil
}

func (e *DumpExtension) HandleBSON(
	bson *bson.D,
	client io.ReadWriter,
	server io.ReadWriter,
	lastError *protocol.LastError,
) (cont bool, err error) {
	fmt.Println("BSON", bson)

	return true, nil
}
