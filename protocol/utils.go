package protocol

import (
	"io"
)

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

func ReadHeader(r io.Reader) (*MsgHeader, error) {
	var d [HeaderLen]byte
	b := d[:]
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	h := MsgHeader{}
	h.FromWire(b)
	return &h, nil
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
