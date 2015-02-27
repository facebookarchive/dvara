package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type testReader struct {
	read func([]byte) (int, error)
}

func (t testReader) Read(b []byte) (int, error) { return t.read(b) }

type testWriter struct {
	write func([]byte) (int, error)
}

func (t testWriter) Write(b []byte) (int, error) { return t.write(b) }

func TestOpStrings(t *testing.T) {
	t.Parallel()
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
	for _, c := range cases {
		if c.OpCode.String() != c.String {
			t.Fatalf("for code %d expected %s but got %s", c.OpCode, c.String, c.OpCode)
		}
	}
}

func TestMsgHeaderString(t *testing.T) {
	t.Parallel()
	m := &MessageHeader{
		OpCode:        OpQuery,
		MessageLength: 10,
		RequestID:     42,
		ResponseTo:    43,
	}
	if m.String() != "opCode:QUERY (2004) msgLen:10 reqID:42 respID:43" {
		t.Fatalf("did not find expected string, instead found: %s", m)
	}
}

func TestCopyEmptyMessage(t *testing.T) {
	t.Parallel()
	msg := MessageHeader{}
	msgBytes := msg.ToWire()
	r := bytes.NewReader(msgBytes)
	var w bytes.Buffer
	if err := CopyMessage(&w, r); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msgBytes, w.Bytes()) {
		t.Fatalf("did not get expected bytes %v got %v", msgBytes, w.Bytes())
	}
}

func TestCopyMessageFromReadError(t *testing.T) {
	t.Parallel()
	expectedErr := errors.New("foo")
	r := testReader{
		read: func(b []byte) (int, error) {
			return 0, expectedErr
		},
	}
	var w bytes.Buffer
	if err := CopyMessage(&w, r); err != expectedErr {
		t.Fatalf("did not get expected error, instead got: %s", err)
	}
}

func TestCopyMessageFromWriteError(t *testing.T) {
	t.Parallel()
	msg := MessageHeader{}
	r := bytes.NewReader(msg.ToWire())
	expectedErr := errors.New("foo")
	w := testWriter{
		write: func(b []byte) (int, error) {
			return 0, expectedErr
		},
	}
	if err := CopyMessage(w, r); err != expectedErr {
		t.Fatalf("did not get expected error, instead got: %s", err)
	}
}

func TestCopyMessageFromWriteLengthError(t *testing.T) {
	t.Parallel()
	msg := MessageHeader{}
	r := bytes.NewReader(msg.ToWire())
	w := testWriter{
		write: func(b []byte) (int, error) {
			return 0, nil
		},
	}
	if err := CopyMessage(w, r); err != errWrite {
		t.Fatalf("did not get expected error, instead got: %s", err)
	}
}

func TestReadDocumentEmpty(t *testing.T) {
	t.Parallel()
	doc, err := ReadDocument(bytes.NewReader([]byte{}))
	if err != io.EOF {
		t.Fatal("did not find expected error")
	}
	if len(doc) != 0 {
		t.Fatal("was expecting an empty document")
	}
}

func TestReadDocumentPartial(t *testing.T) {
	t.Parallel()
	first := true
	r := testReader{
		read: func(b []byte) (int, error) {
			if first {
				first = false
				SetInt32(b, 0, 5)
				return 4, nil
			}
			return 0, io.EOF
		},
	}
	doc, err := ReadDocument(r)
	if err != io.EOF {
		t.Fatalf("did not find expected error, instead got %s %v", err, doc)
	}
	if len(doc) != 0 {
		t.Fatal("was expecting an empty document")
	}
}

func TestReadCString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Data     []byte
		Expected []byte
		Error    error
	}{
		{nil, nil, io.EOF},
		{[]byte{0}, []byte{0}, nil},
		{[]byte{1, 2, 3, 0}, []byte{1, 2, 3, 0}, nil},
		{[]byte{1, 0, 3}, []byte{1, 0}, nil},
	}

	for _, c := range cases {
		cstring, err := ReadCString(bytes.NewReader(c.Data))
		if err != c.Error {
			t.Fatalf("did not find expected error, instead got %s %v", err, cstring)
		}
		if !bytes.Equal(c.Expected, cstring) {
			t.Fatalf("did not find expected %v instead got %v", c.Expected, cstring)
		}
	}
}
