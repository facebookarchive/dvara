package protocol

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ProtocolSuite struct{}

var _ = Suite(&ProtocolSuite{})

func (s *ProtocolSuite) TestMsgHeader_String(c *C) {
	m := &MsgHeader{
		OpCode:        OpQuery,
		MessageLength: 10,
		RequestID:     42,
		ResponseTo:    43,
	}
	c.Assert(m.String(), Equals, "opCode:QUERY (2004) msgLen:10 reqID:42 respID:43")
}
