package protocol

import (
	"bytes"
	"errors"
	"io"

	. "gopkg.in/check.v1"
)

func (s *ProtocolSuite) TestCopyMessageWithEmptyMessage(c *C) {
	msg := MsgHeader{}
	msgBytes := msg.ToWire()
	r := bytes.NewReader(msgBytes)

	var w bytes.Buffer
	err := CopyMessage(&w, r)
	c.Assert(err, IsNil)
	c.Assert(w.Bytes(), DeepEquals, msgBytes)
}

func (s *ProtocolSuite) TestCopyMessageFromReadError(c *C) {
	expectedErr := errors.New("foo")
	r := testReader{
		read: func(b []byte) (int, error) {
			return 0, expectedErr
		},
	}

	var w bytes.Buffer
	err := CopyMessage(&w, r)
	c.Assert(err, Equals, expectedErr)
}

func (s *ProtocolSuite) TestCopyMessageFromWriteError(c *C) {
	msg := MsgHeader{}
	r := bytes.NewReader(msg.ToWire())
	expectedErr := errors.New("foo")
	w := testWriter{
		write: func(b []byte) (int, error) {
			return 0, expectedErr
		},
	}

	err := CopyMessage(w, r)
	c.Assert(err, Equals, expectedErr)

}

func (s *ProtocolSuite) TestCopyMessageFromWriteLengthError(c *C) {
	msg := MsgHeader{}
	r := bytes.NewReader(msg.ToWire())
	w := testWriter{
		write: func(b []byte) (int, error) {
			return 0, nil
		},
	}

	err := CopyMessage(w, r)
	c.Assert(err, Equals, errWrite)
}

func (s *ProtocolSuite) TestReadDocumentEmpty(c *C) {
	doc, err := ReadDocument(bytes.NewReader([]byte{}))
	c.Assert(err, Equals, io.EOF)
	c.Assert(doc, HasLen, 0)
}

func (s *ProtocolSuite) TestReadDocumentPartial(c *C) {
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
	c.Assert(err, Equals, io.EOF)
	c.Assert(doc, HasLen, 0)
}

func (s *ProtocolSuite) TestReadCString(c *C) {
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

	for _, cs := range cases {
		cstring, err := ReadCString(bytes.NewReader(cs.Data))
		c.Assert(err, Equals, cs.Error)
		c.Assert(cstring, DeepEquals, cs.Expected)
	}
}

type testReader struct {
	read func([]byte) (int, error)
}

func (t testReader) Read(b []byte) (int, error) { return t.read(b) }

type testWriter struct {
	write func([]byte) (int, error)
}

func (t testWriter) Write(b []byte) (int, error) { return t.write(b) }
