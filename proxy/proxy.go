package proxy

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mcuadros/exmongodb/protocol"

	"github.com/facebookgo/rpool"
)

const headerLen = 16

var (
	errRSChanged          = errors.New("proxy: replset config changed")
	errZeroMaxConnections = errors.New("proxy: MaxConnections cannot be 0")
	errNormalClose        = errors.New("dvara: normal close")
	errClientReadTimeout  = errors.New("dvara: client read timeout")

	timeInPast = time.Now()
)

// Proxy sends stuff from clients to mongo servers.
type Proxy struct {
	Log Logger
	// Address for incoming client connections
	ProxyAddr string
	// Address for destination Mongo server
	MongoAddr string
	// Maximum number of connections that will be established to each mongo node.
	MaxConnections uint
	// MinIdleConnections is the number of idle server connections we'll keep
	// around.
	MinIdleConnections uint
	// ServerClosePoolSize is the number of goroutines that will handle closing
	// server connections.
	ServerClosePoolSize uint
	// ServerIdleTimeout is the duration after which a server connection will be
	// considered idle.
	ServerIdleTimeout time.Duration
	// GetLastErrorTimeout is how long we'll hold on to an acquired server
	// connection expecting a possibly getLastError call.
	GetLastErrorTimeout time.Duration
	// ClientIdleTimeout is how long until we'll consider a client connection
	// idle and disconnect and release it's resources.
	ClientIdleTimeout time.Duration
	// MessageTimeout is used to determine the timeout for a single message to be
	// proxied.
	MessageTimeout time.Duration

	listener net.Listener

	closed     chan struct{}
	serverPool rpool.Pool

	sync.WaitGroup
}

// String representation for debugging.
func (p *Proxy) String() string {
	return fmt.Sprintf("proxy %s => mongo %s", p.ProxyAddr, p.MongoAddr)
}

// Start the proxy.
func (p *Proxy) Start() error {
	if p.MaxConnections == 0 {
		return errZeroMaxConnections
	}

	if err := p.createListener(); err != nil {
		return err
	}

	p.closed = make(chan struct{})
	p.serverPool = rpool.Pool{
		New:               p.newServerConn,
		CloseErrorHandler: p.serverCloseErrorHandler,
		Max:               p.MaxConnections,
		MinIdle:           p.MinIdleConnections,
		IdleTimeout:       p.ServerIdleTimeout,
		ClosePoolSize:     p.ServerClosePoolSize,
	}

	go p.clientAcceptLoop()

	return nil
}

func (p *Proxy) createListener() error {
	var err error
	if p.listener, err = net.Listen("tcp", p.ProxyAddr); err != nil {
		return err
	}

	return nil
}

// clientAcceptLoop accepts new clients and creates a clientServeLoop for each
// new client that connects to the proxy.
func (p *Proxy) clientAcceptLoop() {
	for {
		p.Add(1)
		c, err := p.listener.Accept()
		if err != nil {
			p.Done()
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			p.Log.Error(err)
			continue
		}

		go p.clientServeLoop(c)
	}
}

// clientServeLoop loops on a single client connected to the proxy and
// dispatches its requests.
func (p *Proxy) clientServeLoop(c net.Conn) {
	c = teeIf(fmt.Sprintf("client %s <=> %s", c.RemoteAddr(), p), c)
	p.Log.Infof("client %s connected to %s", c.RemoteAddr(), p)

	defer func() {
		p.Log.Infof("client %s disconnected from %s", c.RemoteAddr(), p)
		p.Done()
		if err := c.Close(); err != nil {
			p.Log.Error(err)
		}
	}()

	for {
		h, err := p.idleClientReadHeader(c)
		if err != nil {
			if err != errNormalClose {
				p.Log.Error(err)
			}
			return
		}

		serverConn, err := p.getServerConn()
		if err != nil {
			if err != errNormalClose {
				p.Log.Error(err)
			}
			return
		}

		for {
			deadline := time.Now().Add(p.MessageTimeout)
			c.SetDeadline(deadline)
			serverConn.SetDeadline(deadline)

			p.Log.Debugf("proxying message %s from %s for %s", h, c.RemoteAddr(), p)
			if err := p.Handle(h, c, serverConn); err != nil {
				p.serverPool.Discard(serverConn)
				p.Log.Error(err)

				//if err == errRSChanged {
				//	go p.ReplicaSet.Restart()
				//}

				return
			}

			if !h.OpCode.IsMutation() {
				break
			}

			// If the operation we just performed was a mutation, we always make the
			// follow up request on the same server because it's possibly a getLastErr
			// call which expects this behavior.
			h, err = p.gleClientReadHeader(c)
			if err != nil {
				// Client did not make _any_ query within the GetLastErrorTimeout.
				// Return the server to the pool and wait go back to outer loop.
				if err == errClientReadTimeout {
					break
				}
				// Prevent noise of normal client disconnects, but log if anything else.
				if err != errNormalClose {
					p.Log.Error(err)
				}
				// We need to return our server to the pool (it's still good as far
				// as we know).
				p.serverPool.Release(serverConn)
				return
			}
		}
		p.serverPool.Release(serverConn)
	}
}

// getServerConn gets a server connection from the pool.
func (p *Proxy) getServerConn() (net.Conn, error) {
	c, err := p.serverPool.Acquire()
	if err != nil {
		return nil, err
	}
	return c.(net.Conn), nil
}

// We wait for upto ClientIdleTimeout in MessageTimeout increments and keep
// checking if we're waiting to be closed. This ensures that at worse we
// wait for MessageTimeout when closing even when we're idling.
func (p *Proxy) idleClientReadHeader(c net.Conn) (*protocol.MsgHeader, error) {
	return p.clientReadHeader(c, p.ClientIdleTimeout)
}

func (p *Proxy) gleClientReadHeader(c net.Conn) (*protocol.MsgHeader, error) {
	return p.clientReadHeader(c, p.GetLastErrorTimeout)
}

func (p *Proxy) clientReadHeader(c net.Conn, timeout time.Duration) (*protocol.MsgHeader, error) {
	type headerError struct {
		header *protocol.MsgHeader
		error  error
	}
	resChan := make(chan headerError)

	c.SetReadDeadline(time.Now().Add(timeout))
	go func() {
		h, err := protocol.ReadHeader(c)
		resChan <- headerError{header: h, error: err}
	}()

	closed := false
	var response headerError

	select {
	case response = <-resChan:
		// all good
	case <-p.closed:
		closed = true
		c.SetReadDeadline(timeInPast)
		response = <-resChan
	}

	// Successfully read a header.
	if response.error == nil {
		return response.header, nil
	}

	// Client side disconnected.
	if response.error == io.EOF {
		return nil, errNormalClose
	}

	// We hit our ReadDeadline.
	if ne, ok := response.error.(net.Error); ok && ne.Timeout() {
		if closed {
			return nil, errNormalClose
		}
		return nil, errClientReadTimeout
	}

	// Some other unknown error.
	p.Log.Error(response.error)
	return nil, response.error
}

// Stop the proxy.
func (p *Proxy) Stop() error {
	return p.stop(false)
}

func (p *Proxy) stop(hard bool) error {
	if err := p.listener.Close(); err != nil {
		return err
	}
	close(p.closed)
	if !hard {
		p.Wait()
	}
	p.serverPool.Close()
	return nil
}

func (p *Proxy) checkRSChanged() bool {
	return false
	/*
		addrs := p.ReplicaSet.lastState.Addrs()
		r, err := p.ReplicaSet.ReplicaSetStateCreator.FromAddrs(addrs)
		if err != nil {
			p.Log.Errorf("all nodes possibly down?: %s", err)
			return true
		}

		if err := r.AssertEqual(p.ReplicaSet.lastState); err != nil {
			p.Log.Error(err)
			go p.ReplicaSet.Restart()
			return true
		}

		return false
	*/
}

// Open up a new connection to the server. Retry 7 times, doubling the sleep
// each time. This means we'll a total of 12.75 seconds with the last wait
// being 6.4 seconds.
func (p *Proxy) newServerConn() (io.Closer, error) {
	retrySleep := 50 * time.Millisecond
	for retryCount := 7; retryCount > 0; retryCount-- {
		c, err := net.Dial("tcp", p.MongoAddr)
		if err == nil {
			return c, nil
		}
		p.Log.Error(err)

		// abort if rs changed
		if p.checkRSChanged() {
			return nil, errNormalClose
		}
		time.Sleep(retrySleep)
		retrySleep = retrySleep * 2
	}
	return nil, fmt.Errorf("could not connect to %s", p.MongoAddr)
}

func (p *Proxy) serverCloseErrorHandler(err error) {
	p.Log.Error(err)
}

var teeIfEnable = os.Getenv("MONGOPROXY_TEE") == "1"

type teeConn struct {
	context string
	net.Conn
}

func (t teeConn) Read(b []byte) (int, error) {
	n, err := t.Conn.Read(b)
	if n > 0 {
		fmt.Fprintf(os.Stdout, "READ %s: %s %v\n", t.context, b[0:n], b[0:n])
	}
	return n, err
}

func (t teeConn) Write(b []byte) (int, error) {
	n, err := t.Conn.Write(b)
	if n > 0 {
		fmt.Fprintf(os.Stdout, "WRIT %s: %s %v\n", t.context, b[0:n], b[0:n])
	}
	return n, err
}

func teeIf(context string, c net.Conn) net.Conn {
	if teeIfEnable {
		return teeConn{
			context: context,
			Conn:    c,
		}
	}
	return c
}
