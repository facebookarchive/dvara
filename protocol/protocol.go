package protocol

// Look at http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/ for the protocol.

// OpCode allow identifying the type of operation:
//
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
type OpCode int32

// String returns a human readable representation of the OpCode.
func (c OpCode) String() string {
	switch c {
	default:
		return "UNKNOWN"
	case OpReply:
		return "REPLY"
	case OpMessage:
		return "MESSAGE"
	case OpUpdate:
		return "UPDATE"
	case OpInsert:
		return "INSERT"
	case Reserved:
		return "RESERVED"
	case OpQuery:
		return "QUERY"
	case OpGetMore:
		return "GET_MORE"
	case OpDelete:
		return "DELETE"
	case OpKillCursors:
		return "KILL_CURSORS"
	}
}

// IsMutation tells us if the operation will mutate data. These operations can
// be followed up by a getLastErr operation.
func (c OpCode) IsMutation() bool {
	return c == OpInsert || c == OpUpdate || c == OpDelete
}

// HasResponse tells us if the operation will have a response from the server.
func (c OpCode) HasResponse() bool {
	return c == OpQuery || c == OpGetMore
}

// The full set of known request op codes:
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
const (
	OpReply       = OpCode(1)
	OpMessage     = OpCode(1000)
	OpUpdate      = OpCode(2001)
	OpInsert      = OpCode(2002)
	Reserved      = OpCode(2003)
	OpQuery       = OpCode(2004)
	OpGetMore     = OpCode(2005)
	OpDelete      = OpCode(2006)
	OpKillCursors = OpCode(2007)
)
