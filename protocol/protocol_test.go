package protocol

import (
	. "gopkg.in/check.v1"
)

func (s *ProtocolSuite) TestOpCode_Strings(c *C) {
	cases := []struct {
		OpCode OpCode
		String string
	}{
		{OpCode(0), "UNKNOWN"},
		{OpReply, "REPLY"},
		{OpMessage, "MESSAGE"},
		{OpUpdate, "UPDATE"},
		{OpInsert, "INSERT"},
		{Reserved, "RESERVED"},
		{OpQuery, "QUERY"},
		{OpGetMore, "GET_MORE"},
		{OpDelete, "DELETE"},
		{OpKillCursors, "KILL_CURSORS"},
	}
	for _, cs := range cases {
		c.Assert(cs.OpCode.String(), Equals, cs.String)
	}
}
