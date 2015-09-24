// Package gmetric provides a client for the ganglia gmetric API.
package gmetric

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/facebookgo/flag.addrs"
)

var (
	zeroByte       = []byte{byte(0)}
	errNoAddrs     = errors.New("gmetric: no addrs provided")
	errNotOpen     = errors.New("gmetric: client not opened")
	errNoName      = errors.New("gmetric: metric has no name")
	errNoValueType = errors.New("gmetric: metric has no ValueType")
)

type slopeType string

// The slope types supported by Ganglia.
const (
	SlopeZero     = slopeType("zero")
	SlopePositive = slopeType("positive")
	SlopeNegative = slopeType("negative")
	SlopeBoth     = slopeType("both")
)

func (s slopeType) value() uint32 {
	switch s {
	case SlopeZero:
		return 0
	case SlopePositive:
		return 1
	case SlopeNegative:
		return 2
	case SlopeBoth:
		return 3
	}
	return 4
}

type valueType string

// The value types supported by Ganglia.
const (
	ValueString  = valueType("string")
	ValueUint8   = valueType("uint8")
	ValueInt8    = valueType("int8")
	ValueUint16  = valueType("uint16")
	ValueInt16   = valueType("int16")
	ValueUint32  = valueType("uint32")
	ValueInt32   = valueType("int32")
	ValueFloat32 = valueType("float")
	ValueFloat64 = valueType("double")
)

// MultiError represents a collection of errors.
type MultiError []error

// Returns a concatenation of all the contained errors.
func (m MultiError) Error() string {
	var buf bytes.Buffer
	buf.WriteString("gmetric: multi-error:")
	for _, e := range m {
		buf.WriteRune('\n')
		buf.WriteString(e.Error())
	}
	return buf.String()
}

// A Client represents a set of connections to write metrics to. The Client is
// itself a Writer which writes the given bytes to all open connections.
type Client struct {
	io.Writer

	// The target addresses or in gmond.conf parlance the udp_send_channels.
	Addr []net.Addr

	// The actual hostname for the machine. If empty the default will be set
	// based on os.Hostname.
	Host string

	// Optional spoof name for the machine. Since the default is reverse DNS this
	// allows for overriding the hostname to make it useful.
	Spoof string

	// Also known as TMax, it defines the max time interval between which the
	// daemon will expect updates. This should map to how often you publish the
	// metric.
	TickInterval time.Duration

	// Also known as DMax, it defines the lifetime for the metric. That is, once
	// the last received metric is older than the defined value it will become
	// eligible for garbage collection.
	Lifetime time.Duration

	conn []net.Conn
}

// Metric configuration.
type Metric struct {
	// The name is used as the file name, and also the title unless one is
	// explicitly provided.
	Name string

	// The title is for human consumption and is shown atop the graph.
	Title string

	// Descriptions serve as documentation.
	Description string

	// The groups ensure your metric is kept alongside sibling metrics.
	Groups []string

	// The units are shown in the graph to provide context to the numbers.
	Units string

	// The actual hostname for the machine.
	Host string

	// Optional spoof name for the machine. Since the default is reverse DNS this
	// allows for overriding the hostname to make it useful.
	Spoof string

	// Defines the value type. You must specify one of the predefined constants.
	ValueType valueType

	// Defines the slope type. You must specify one of the predefined constants.
	Slope slopeType

	// Also known as TMax, it defines the max time interval between which the
	// daemon will expect updates. This should map to how often you publish the
	// metric.
	TickInterval time.Duration

	// Also known as DMax, it defines the lifetime for the metric. That is, once
	// the last received metric is older than the defined value it will become
	// eligible for garbage collection.
	Lifetime time.Duration
}

// Writes a metadata packet for the Metric.
func (m *Metric) writeMeta(c *Client, w io.Writer) (err error) {
	pw := &panickyWriter{Writer: w}
	defer func() {
		if r := recover(); r != nil {
			if r == errPanickyWriter {
				err = pw.Error
			} else {
				panic(r)
			}
		}
	}()

	writeUint32(pw, 128) // identifies meta
	m.writeHead(c, pw)
	writeString(pw, string(m.ValueType))
	writeString(pw, m.Name)
	writeString(pw, m.Units)
	writeUint32(pw, m.Slope.value())

	tick := m.TickInterval.Seconds()
	if tick == 0 {
		writeUint32(pw, uint32(c.TickInterval.Seconds()))
	} else {
		writeUint32(pw, uint32(tick))
	}

	life := m.Lifetime.Seconds()
	if life == 0 {
		writeUint32(pw, uint32(c.Lifetime.Seconds()))
	} else {
		writeUint32(pw, uint32(life))
	}

	var extras [][2]string
	if m.Title != "" {
		extras = append(extras, [2]string{"TITLE", m.Title})
	}
	if m.Description != "" {
		extras = append(extras, [2]string{"DESC", m.Description})
	}

	spoof := m.Spoof
	if spoof == "" {
		spoof = c.Spoof
	}
	if spoof != "" {
		extras = append(extras, [2]string{"SPOOF_HOST", spoof})
	}

	for _, group := range m.Groups {
		extras = append(extras, [2]string{"GROUP", group})
	}
	writeExtras(pw, extras)
	return
}

// Writes a value packet for the given value. The value will be encoded based
// on the configured ValueType.
func (m *Metric) writeValue(c *Client, w io.Writer, val interface{}) (err error) {
	pw := &panickyWriter{Writer: w}
	defer func() {
		if r := recover(); r != nil {
			if r == errPanickyWriter {
				err = pw.Error
			} else {
				panic(r)
			}
		}
	}()

	writeUint32(pw, 133) // identifies a value
	m.writeHead(c, pw)
	writeString(pw, "%s")
	writeString(pw, fmt.Sprint(val))
	return
}

func (m *Metric) writeHead(c *Client, w io.Writer) {
	host := m.Host
	if host == "" {
		host = c.Host
	}

	spoof := m.Spoof
	if spoof == "" {
		spoof = c.Spoof
	}

	hasSpoof := spoof != ""
	if hasSpoof {
		writeString(w, spoof)
	} else {
		writeString(w, host)
	}
	writeString(w, m.Name)
	if hasSpoof {
		writeUint32(w, 1)
	} else {
		writeUint32(w, 0)
	}
}

func (c *Client) writeCheck(m *Metric) error {
	if c.Writer == nil {
		return errNotOpen
	}
	if m.Name == "" {
		return errNoName
	}
	if string(m.ValueType) == "" {
		return errNoValueType
	}
	return nil
}

// WriteMeta writes the Metric metadata.
func (c *Client) WriteMeta(m *Metric) error {
	if err := c.writeCheck(m); err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := m.writeMeta(c, &buf); err != nil {
		return err
	}
	if _, err := c.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// WriteValue writes a value for the Metric.
func (c *Client) WriteValue(m *Metric, val interface{}) error {
	if err := c.writeCheck(m); err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := m.writeValue(c, &buf, val); err != nil {
		return err
	}
	if _, err := c.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// Open the connections. If an error is returned it will be a MultiError.
func (c *Client) Open() error {
	if len(c.Addr) == 0 {
		return errNoAddrs
	}

	if c.Host == "" {
		c.Host, _ = os.Hostname()
	}

	var errs MultiError
	var writers []io.Writer
	for _, addr := range c.Addr {
		s, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			errs = append(errs, err)
			continue
		}
		c.conn = append(c.conn, s)
		writers = append(writers, s)
	}
	c.Writer = io.MultiWriter(writers...)

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// Close the connections. If an error is returned it will be a MultiError.
func (c *Client) Close() error {
	if len(c.Addr) == 0 {
		return errNoAddrs
	}

	var errs MultiError
	for _, conn := range c.conn {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// ClientFromFlag defines a new Client via Flags. Note you must call
// client.Open() before using it.
func ClientFromFlag(name string) *Client {
	c := &Client{}
	hostname, _ := os.Hostname()
	flag.StringVar(
		&c.Host,
		name+".host",
		hostname,
		"hostname for ganglia",
	)
	flag.StringVar(
		&c.Spoof,
		name+".spoof",
		"",
		"spoof hostname for ganglia",
	)
	flag.DurationVar(
		&c.TickInterval,
		name+".tick-interval",
		time.Minute,
		"tick interval for ganglia",
	)
	flag.DurationVar(
		&c.Lifetime,
		name+".lifetime",
		time.Hour*24*30, // 30 days
		"metrics lifetime for ganglia",
	)
	addrs.FlagManyVar(
		&c.Addr,
		name+".addrs",
		"udp:127.0.0.1:8649",
		"comma separated list of net:host:port triples of ganglia leaf nodes",
	)
	return c
}

func writeUint32(w io.Writer, val uint32) {
	w.Write([]byte{
		byte(val >> 24 & 0xff),
		byte(val >> 16 & 0xff),
		byte(val >> 8 & 0xff),
		byte(val & 0xff),
	})
}

func writeString(w io.Writer, val string) {
	l := uint32(len(val))
	writeUint32(w, l)
	fmt.Fprint(w, val)
	offset := l % 4
	if offset != 0 {
		for j := offset; j < 4; j++ {
			w.Write(zeroByte)
		}
	}
}

func writeExtras(w io.Writer, extras [][2]string) {
	writeUint32(w, uint32(len(extras)))
	for _, p := range extras {
		writeString(w, p[0])
		writeString(w, p[1])
	}
}

var errPanickyWriter = errors.New("panicky-writer sentinel")

// Panicky Writer provides a io.Writer that panics whenever the underlying
// writer returns an error. This allows for localized panic/recover for less
// verbose error handling internally. DO NOT expose this without a
// corresponding recover().
type panickyWriter struct {
	io.Writer
	Count int
	Error error
}

func (p *panickyWriter) Write(b []byte) (int, error) {
	n, err := p.Writer.Write(b)
	p.Count += n
	if err != nil {
		p.Error = err
		panic(errPanickyWriter)
	}
	return n, nil
}
