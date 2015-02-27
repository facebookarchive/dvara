package protocol

import (
	"bytes"
)

// LastError holds the last known error.
type LastError struct {
	Header *MessageHeader
	Rest   bytes.Buffer
}

// Exists returns true if this instance contains a cached error.
func (l *LastError) Exists() bool {
	return l.Header != nil
}

// Reset resets the stored error clearing it.
func (l *LastError) Reset() {
	l.Header = nil
	l.Rest.Reset()
}
