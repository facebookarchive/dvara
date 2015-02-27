package protocol

import (
	"errors"
	"fmt"
	"io"
)

const HeaderLen = 16

var (
	errWrite = errors.New("incorrect number of bytes written")
)

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

// MessageHeader is the mongo MessageHeader
type MessageHeader struct {
	// MessageLength is the total message size, including this header
	MessageLength int32
	// RequestID is the identifier for this miessage
	RequestID int32
	// ResponseTo is the RequestID of the message being responded to. used in DB responses
	ResponseTo int32
	// OpCode is the request type, see consts above.
	OpCode OpCode
}

// ToWire converts the MessageHeader to the wire protocol
func (m MessageHeader) ToWire() []byte {
	var d [HeaderLen]byte
	b := d[:]
	SetInt32(b, 0, m.MessageLength)
	SetInt32(b, 4, m.RequestID)
	SetInt32(b, 8, m.ResponseTo)
	SetInt32(b, 12, int32(m.OpCode))
	return b
}

// FromWire reads the wirebytes into this object
func (m *MessageHeader) FromWire(b []byte) {
	m.MessageLength = GetInt32(b, 0)
	m.RequestID = GetInt32(b, 4)
	m.ResponseTo = GetInt32(b, 8)
	m.OpCode = OpCode(GetInt32(b, 12))
}

func (m *MessageHeader) WriteTo(w io.Writer) error {
	b := m.ToWire()
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return errWrite
	}
	return nil
}

// String returns a string representation of the message header. Useful for debugging.
func (m *MessageHeader) String() string {
	return fmt.Sprintf(
		"opCode:%s (%d) msgLen:%d reqID:%d respID:%d",
		m.OpCode,
		m.OpCode,
		m.MessageLength,
		m.RequestID,
		m.ResponseTo,
	)
}

func ReadHeader(r io.Reader) (*MessageHeader, error) {
	var d [HeaderLen]byte
	b := d[:]
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	h := MessageHeader{}
	h.FromWire(b)
	return &h, nil
}

// copyMessage copies reads & writes an entire message.
func CopyMessage(w io.Writer, r io.Reader) error {
	h, err := ReadHeader(r)
	if err != nil {
		return err
	}
	if err := h.WriteTo(w); err != nil {
		return err
	}
	_, err = io.CopyN(w, r, int64(h.MessageLength-HeaderLen))
	return err
}

// readDocument read an entire BSON document. This document can be used with
// bson.Unmarshal.
func ReadDocument(r io.Reader) ([]byte, error) {
	var sizeRaw [4]byte
	if _, err := io.ReadFull(r, sizeRaw[:]); err != nil {
		return nil, err
	}
	size := GetInt32(sizeRaw[:], 0)
	doc := make([]byte, size)
	SetInt32(doc, 0, size)
	if _, err := io.ReadFull(r, doc[4:]); err != nil {
		return nil, err
	}
	return doc, nil
}

const x00 = byte(0)

// ReadCString reads a null turminated string as defined by BSON from the
// reader. Note, the return value includes the trailing null byte.
func ReadCString(r io.Reader) ([]byte, error) {
	var b []byte
	var n [1]byte
	for {
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return nil, err
		}
		b = append(b, n[0])
		if n[0] == x00 {
			return b, nil
		}
	}
}

// all data in the MongoDB wire protocol is little-endian.
// all the read/write functions below are little-endian.
func GetInt32(b []byte, pos int) int32 {
	return (int32(b[pos+0])) |
		(int32(b[pos+1]) << 8) |
		(int32(b[pos+2]) << 16) |
		(int32(b[pos+3]) << 24)
}

func SetInt32(b []byte, pos int, i int32) {
	b[pos] = byte(i)
	b[pos+1] = byte(i >> 8)
	b[pos+2] = byte(i >> 16)
	b[pos+3] = byte(i >> 24)
}
