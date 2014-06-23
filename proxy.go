package dvara

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const headerLen = 16

var (
	errZeroMaxConnections = errors.New("dvara: MaxConnections cannot be 0")
	errNormalClose        = errors.New("dvara: normal close")
	errClientReadTimeout  = errors.New("dvara: client read timeout")
)

// Proxy sends stuff from clients to mongo servers.
type Proxy struct {
	Log            Logger
	ReplicaSet     *ReplicaSet
	ClientListener net.Listener // Listener for incoming client connections
	ProxyAddr      string       // Address for incoming client connections
	MongoAddr      string       // Address for destination Mongo server

	wg                  sync.WaitGroup
	closed              bool
	closedMutex         sync.RWMutex
	serverConnPool      chan net.Conn
	numServerConns      uint
	numServerConnsMutex sync.Mutex
}

// String representation for debugging.
func (p *Proxy) String() string {
	return fmt.Sprintf("proxy %s => mongo %s", p.ProxyAddr, p.MongoAddr)
}

// Start the proxy.
func (p *Proxy) Start() error {
	if p.ReplicaSet.MaxConnections == 0 {
		return errZeroMaxConnections
	}

	p.closedMutex.Lock()
	p.closed = false
	p.closedMutex.Unlock()

	p.serverConnPool = make(chan net.Conn, p.ReplicaSet.MaxConnections)
	go p.clientAcceptLoop()

	return nil
}

// Stop the proxy.
func (p *Proxy) Stop() error {
	return p.stop(false)
}

func (p *Proxy) stop(hard bool) error {
	p.closedMutex.Lock()
	p.closed = true
	p.closedMutex.Unlock()

	if err := p.ClientListener.Close(); err != nil {
		return err
	}
	if !hard {
		p.wg.Wait()
	}

	go func() {
		for c := range p.serverConnPool {
			p.ReplicaSet.ServerDisconnected.Mark(1)
			p.ReplicaSet.ServersConnected.Dec(1)
			if err := c.Close(); err != nil {
				p.ReplicaSet.ServerDisconnectFailure.Mark(1)
				p.Log.Error(err)
			}
		}
	}()
	close(p.serverConnPool)

	return nil
}

func (p *Proxy) checkRSChanged() bool {
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
}

// isClosed returns true if we're in the process of closing the proxy.
func (p *Proxy) isClosed() bool {
	p.closedMutex.RLock()
	defer p.closedMutex.RUnlock()
	return p.closed
}

// refreshServerConn will disconnect the given conn if possible, and
// return a fresh connection.
func (p *Proxy) refreshServerConn(oldC net.Conn) (net.Conn, error) {
	if oldC != nil {
		p.ReplicaSet.ServerDisconnected.Mark(1)
		p.ReplicaSet.ServersConnected.Dec(1)
		if err := oldC.Close(); err != nil {
			p.ReplicaSet.ServerDisconnectFailure.Mark(1)
			p.Log.Error(err)
		}
		if p.checkRSChanged() {
			return nil, errNormalClose
		}
	}

	// We retry indefinitely, with an exponential sleep in between retries.
	retryCount := int64(0)
	retrySleep := 50 * time.Millisecond

	for {
		c, err := net.Dial("tcp", p.MongoAddr)
		if err == nil {
			p.ReplicaSet.ServerConnected.Mark(1)
			p.ReplicaSet.ServersConnected.Inc(1)
			return c, nil
		}
		p.ReplicaSet.ServerConnectFailure.Mark(1)
		p.Log.Error(err)

		// If we're closing, then we're done.
		if p.isClosed() {
			return nil, errNormalClose
		}

		if p.checkRSChanged() {
			return nil, errNormalClose
		}
		time.Sleep(retrySleep)

		retryCount++
		retrySleep = time.Duration((2^retryCount)*int64(retrySleep) + rand.Int63n(int64(retrySleep)))
	}
}

// getServerConn returns a server connection from our pool, but will allow us
// to identify if we don't have enough connections for our clients.
func (p *Proxy) getServerConn() net.Conn {
	logged := false
	maxEstablished := false
	for {
		select {
		case c := <-p.serverConnPool:
			return c
		default:
			if p.isClosed() {
				return nil // this is handled by the caller
			}

			// Hot path to only have to Lock once if we've already opened
			// MaxConnections.
			if maxEstablished {
				p.ReplicaSet.ServerPoolRecvBlocked.Mark(1)
				continue
			}

			p.numServerConnsMutex.Lock()

			// This is the case where we're blocked and can't open up a new
			// connection since we've opened up MaxConnections.
			if p.numServerConns >= p.ReplicaSet.MaxConnections {
				p.numServerConnsMutex.Unlock()
				maxEstablished = true
				p.ReplicaSet.ServerPoolRecvBlocked.Mark(1)
				if !logged {
					logged = true
					p.Log.Warn("serverConnPool recv blocked")
				}
				continue
			}

			// We're going to open up a new connection to the server.
			c, err := p.refreshServerConn(nil)
			if err != nil {
				p.numServerConnsMutex.Unlock()
				// errNormalClose here means we're closing the proxy, so we return a
				// nil net.Conn, which is handled by callers.
				if err == errNormalClose {
					return nil
				}
				p.Log.Errorf("ignoring server connection failure: %s", err)
				continue
			}
			p.numServerConns++
			p.numServerConnsMutex.Unlock()
			return c
		}
	}
}

// putServerConn returns a server connection to our pool, but will allow us
// to identify if we block on this indicating we need a bigger channel.
func (p *Proxy) putServerConn(c net.Conn) {
	logged := false
	for {
		select {
		case p.serverConnPool <- c:
			return
		default:
			if p.isClosed() {
				return
			}
			p.ReplicaSet.ServerPoolSendBlocked.Mark(1)
			if !logged {
				logged = true
				p.Log.Warn("serverConnPool send blocked")
			}
		}
	}
}

// proxyMessage proxies a message, possibly it's response, and possibly a
// follow up call.
func (p *Proxy) proxyMessage(
	h *messageHeader,
	client net.Conn,
	server net.Conn,
	lastError *LastError,
) error {

	p.Log.Infof("proxying message %s from %s for %s", h, client.RemoteAddr(), p)
	deadline := time.Now().Add(p.ReplicaSet.MessageTimeout)
	server.SetDeadline(deadline)
	client.SetDeadline(deadline)

	// OpQuery may need to be transformed and need special handling in order to
	// make the proxy transparent.
	if h.OpCode == OpQuery {
		p.ReplicaSet.MessageWithResponse.Mark(1)
		return p.ReplicaSet.ProxyQuery.Proxy(h, client, server, lastError)
	}

	// Anything besides a getlasterror call (which requires an OpQuery) resets
	// the lastError.
	if lastError.Exists() {
		p.Log.Info("reset getLastError cache")
		lastError.Reset()
	}

	// For other Ops we proxy the header & raw body over.
	if err := h.WriteTo(server); err != nil {
		p.Log.Error(err)
		return err
	}

	if _, err := io.CopyN(server, client, int64(h.MessageLength-headerLen)); err != nil {
		p.Log.Error(err)
		return err
	}

	// For Ops with responses we proxy the raw response message over.
	if h.OpCode.HasResponse() {
		p.ReplicaSet.MessageWithResponse.Mark(1)
		if err := copyMessage(client, server); err != nil {
			p.Log.Error(err)
			return err
		}
	}

	return nil
}

// clientAcceptLoop accepts new clients and creates a clientServeLoop for each
// new client that connects to the proxy.
func (p *Proxy) clientAcceptLoop() {
	for {
		p.wg.Add(1)
		c, err := p.ClientListener.Accept()
		if err != nil {
			p.wg.Done()
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
	p.ReplicaSet.ClientConnected.Mark(1)
	p.ReplicaSet.ClientsConnected.Inc(1)
	defer func() {
		p.ReplicaSet.ClientsConnected.Dec(1)
		p.Log.Infof("client %s disconnected from %s", c.RemoteAddr(), p)
		p.wg.Done()
		if err := c.Close(); err != nil {
			p.Log.Error(err)
		}
	}()

	var lastError LastError
	for {
		h, err := p.idleClientReadHeader(c)
		if err != nil {
			if err != errNormalClose {
				p.Log.Error(err)
			}
			return
		}

		serverConn := p.getServerConn()
		if serverConn == nil {
			// This is a legit scenario when we're closing since the closed
			// serverConnPool chan will return the default nil value.
			if !p.isClosed() {
				p.Log.Error("dvara: got nil from getServerConn")
			}
			return
		}

		t := p.ReplicaSet.ServerConnHeld.Start()
		for {
			err := p.proxyMessage(h, c, serverConn, &lastError)
			if err != nil {
				p.Log.Error(err)

				serverConn, sErr := p.refreshServerConn(serverConn)
				if sErr != nil {
					p.Log.Error(sErr)
				} else {
					p.putServerConn(serverConn)
				}

				p.ReplicaSet.MessageProxyFailure.Mark(1)
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					p.ReplicaSet.MessageTimeoutHit.Mark(1)
				}
				if err == errRSChanged {
					go p.ReplicaSet.Restart()
				}
				return
			}

			if !h.OpCode.IsMutation() {
				break
			}

			// If the operation we just performed was a mutation, we always make the
			// follow up request on the same server because it's possibly a getLastErr
			// call which expects this behavior.
			p.ReplicaSet.MessageWithMutation.Mark(1)
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
				p.putServerConn(serverConn)
				return
			}
		}
		p.putServerConn(serverConn)
		t.Stop()
		p.ReplicaSet.MessageProxySuccess.Mark(1)
	}
}

// We wait for upto ClientIdleTimeout in MessageTimeout increments and keep
// checking if we're waiting to be closed. This ensures that at worse we
// wait for MessageTimeout when closing even when we're idling.
func (p *Proxy) idleClientReadHeader(c net.Conn) (*messageHeader, error) {
	h, err := p.clientReadHeader(c, p.ReplicaSet.ClientIdleTimeout)
	if err == errClientReadTimeout {
		p.ReplicaSet.ClientIdleTimeoutHit.Mark(1)
	}
	return h, err
}

func (p *Proxy) gleClientReadHeader(c net.Conn) (*messageHeader, error) {
	h, err := p.clientReadHeader(c, p.ReplicaSet.GetLastErrorTimeout)
	if err == errClientReadTimeout {
		p.ReplicaSet.GetLastErrorTimeoutHit.Mark(1)
	}
	return h, err
}

func (p *Proxy) clientReadHeader(c net.Conn, timeout time.Duration) (*messageHeader, error) {
	t := p.ReplicaSet.ClientReadHeaderWait.Start()
	now := time.Now()
	deadline := now.Add(timeout)
	for now.Before(deadline) {
		// Proxy.Stop has been triggered.
		if p.isClosed() {
			p.ReplicaSet.ClientCleanDisconnect.Mark(1)
			return nil, errNormalClose
		}

		// TODO deal with partial reads?
		// Try to read the header.
		c.SetReadDeadline(now.Add(p.ReplicaSet.MessageTimeout))
		h, err := readHeader(c)

		// Successfully read a header.
		if err == nil {
			t.Stop()
			return h, nil
		}

		// Client side disconnected.
		if err == io.EOF {
			p.ReplicaSet.ClientCleanDisconnect.Mark(1)
			return nil, errNormalClose
		}

		// We hit our ReadDeadline (1 round of MessageTimeout)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			now = time.Now()
			continue
		}

		// Some other unknown error.
		p.ReplicaSet.ClientErrorDisconnect.Mark(1)
		p.Log.Error(err)
		return nil, err
	}
	return nil, errClientReadTimeout
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
