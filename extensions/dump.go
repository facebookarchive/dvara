package extensions

import (
	"fmt"
	"io"

	"github.com/mcuadros/exmongodb/protocol"

	"gopkg.in/mgo.v2/bson"
)

type DumpExtension struct{}

func (e *DumpExtension) HandleOp(
	header *protocol.MsgHeader,
	client io.ReadWriter,
	server io.ReadWriter,
) (cont bool, err error) {
	//	fmt.Println("DUMP", header)

	return true, nil
}

func (e *DumpExtension) HandleBSON(
	d *bson.D,
	client io.ReadWriter,
	server io.ReadWriter,
) (cont bool, err error) {
	return true, nil

	for _, v := range *d {
		if v.Name == "documents" {
			values := v.Value.([]interface{})
			m := values[0].(bson.D).Map()
			fmt.Println("key", m)
		}
	}

	return true, nil
}
