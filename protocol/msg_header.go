package protocol

import (
	"errors"
	"fmt"
	"io"
)

const HeaderLen = 16

var errWrite = errors.New("incorrect number of bytes written")

// MsgHeader is the mongo MsgHeader
type MsgHeader struct {
	// MessageLength is the total message size, including this header
	MessageLength int32
	// RequestID is the identifier for this miessage
	RequestID int32
	// ResponseTo is the RequestID of the message being responded to. used in DB responses
	ResponseTo int32
	// OpCode is the request type, see consts above.
	OpCode OpCode
}

// ToWire converts the MsgHeader to the wire protocol
func (m MsgHeader) ToWire() []byte {
	var d [HeaderLen]byte
	b := d[:]
	SetInt32(b, 0, m.MessageLength)
	SetInt32(b, 4, m.RequestID)
	SetInt32(b, 8, m.ResponseTo)
	SetInt32(b, 12, int32(m.OpCode))
	return b
}

// FromWire reads the wirebytes into this object
func (m *MsgHeader) FromWire(b []byte) {
	m.MessageLength = GetInt32(b, 0)
	m.RequestID = GetInt32(b, 4)
	m.ResponseTo = GetInt32(b, 8)
	m.OpCode = OpCode(GetInt32(b, 12))
}

func (m *MsgHeader) WriteTo(w io.Writer) error {
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
func (m *MsgHeader) String() string {
	return fmt.Sprintf(
		"opCode:%s (%d) msgLen:%d reqID:%d respID:%d",
		m.OpCode,
		m.OpCode,
		m.MessageLength,
		m.RequestID,
		m.ResponseTo,
	)
}
