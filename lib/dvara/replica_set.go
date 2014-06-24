package dvara

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/facebookgo/gangliamr"
	"github.com/facebookgo/metrics"
	"github.com/facebookgo/stackerr"
)

// Logger allows for simple text logging.
type Logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

var errNoAddrsGiven = errors.New("dvara: no seed addresses given for ReplicaSet")

// ReplicaSet manages the real => proxy address mapping.
// NewReplicaSet returns the ReplicaSet given the list of seed servers. It is
// required for the seed servers to be a strict subset of the actual members if
// they are reachable. That is, if two of the addresses are members of
// different replica sets, it will be considered an error.
type ReplicaSet struct {
	Log                    Logger                  `inject:""`
	ReplicaSetStateCreator *ReplicaSetStateCreator `inject:""`
	ProxyQuery             *ProxyQuery             `inject:""`

	// Comma separated list of mongo addresses. This is the list of "seed"
	// servers, and one of two conditions must be met for each entry here -- it's
	// either alive and part of the same replica set as all others listed, or is
	// not reachable.
	Addrs string

	// PortStart and PortEnd define the port range within which proxies will be
	// allocated.
	PortStart int
	PortEnd   int

	// Maximum number of connections that will be established to each mongo node.
	MaxConnections uint

	// ClientIdleTimeout is how long until we'll consider a client connection
	// idle and disconnect and release it's resources.
	ClientIdleTimeout time.Duration

	// GetLastErrorTimeout is how long we'll hold on to an acquired server
	// connection expecting a possibly getLastError call.
	GetLastErrorTimeout time.Duration

	// MessageTimeout is used to determine the timeout for a single message to be
	// proxied.
	MessageTimeout time.Duration

	ClientCleanDisconnect   metrics.Meter
	ClientConnected         metrics.Meter
	ClientErrorDisconnect   metrics.Meter
	ClientIdleTimeoutHit    metrics.Meter
	ClientReadHeaderWait    metrics.Timer
	ClientsConnected        metrics.Counter
	GetLastErrorTimeoutHit  metrics.Meter
	MessageProxyFailure     metrics.Meter
	MessageProxySuccess     metrics.Meter
	MessageTimeoutHit       metrics.Meter
	MessageWithMutation     metrics.Meter
	MessageWithResponse     metrics.Meter
	ServerConnHeld          metrics.Timer
	ServerConnectFailure    metrics.Meter
	ServerConnected         metrics.Meter
	ServerDisconnectFailure metrics.Meter
	ServerDisconnected      metrics.Meter
	ServerPoolRecvBlocked   metrics.Meter
	ServerPoolSendBlocked   metrics.Meter
	ServersConnected        metrics.Counter

	proxyToReal map[string]string
	realToProxy map[string]string
	proxies     map[string]*Proxy
	restarter   *sync.Once
	lastState   *ReplicaSetState
}

// RegisterMetrics registers the available metrics.
func (r *ReplicaSet) RegisterMetrics(registry *gangliamr.Registry) {
	gangliaGroup := []string{"dvara"}
	r.ClientConnected = &gangliamr.Meter{
		Name:   "client_connected",
		Title:  "Client Connected",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientConnected)

	r.ClientsConnected = &gangliamr.Counter{
		Name:   "clients_connected",
		Title:  "Client Connected",
		Units:  "conn",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientsConnected)

	r.ClientCleanDisconnect = &gangliamr.Meter{
		Name:   "client_clean_disconnect",
		Title:  "Client Disconnected Cleanly",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientCleanDisconnect)

	r.ClientErrorDisconnect = &gangliamr.Meter{
		Name:   "client_error_disconnect",
		Title:  "Client Disconnected With Error",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientErrorDisconnect)

	r.ClientReadHeaderWait = &gangliamr.Timer{
		Name:   "client_read_header_wait",
		Title:  "Duration a Client ReadHeader was waiting",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientReadHeaderWait)

	r.ClientIdleTimeoutHit = &gangliamr.Meter{
		Name:   "client_idle_timeout_hit",
		Title:  "Client Idle Timeout Hit",
		Units:  "timeout/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ClientIdleTimeoutHit)

	r.ServersConnected = &gangliamr.Counter{
		Name:   "servers_connected",
		Title:  "Servers Connected",
		Units:  "conn",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServersConnected)

	r.ServerConnected = &gangliamr.Meter{
		Name:   "server_connected",
		Title:  "Server Connected",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerConnected)

	r.ServerConnectFailure = &gangliamr.Meter{
		Name:   "server_connect_failure",
		Title:  "Server Connect Failure",
		Units:  "failure/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerConnectFailure)

	r.ServerDisconnected = &gangliamr.Meter{
		Name:   "server_disconnected",
		Title:  "Server Disconnected",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerDisconnected)

	r.ServerDisconnectFailure = &gangliamr.Meter{
		Name:   "server_disconnect_failure",
		Title:  "Server Disconnect Failure",
		Units:  "failure/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerDisconnectFailure)

	r.ServerPoolRecvBlocked = &gangliamr.Meter{
		Name:   "server_pool_recv_blocked",
		Title:  "Server Pool Recv Blocked",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerPoolRecvBlocked)

	r.ServerPoolSendBlocked = &gangliamr.Meter{
		Name:   "server_pool_send_blocked",
		Title:  "Server Pool Send Blocked",
		Units:  "conn/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerPoolSendBlocked)

	r.MessageTimeoutHit = &gangliamr.Meter{
		Name:   "message_timeout_hit",
		Title:  "Message Timeout Hit",
		Units:  "message/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.MessageTimeoutHit)

	r.MessageProxySuccess = &gangliamr.Meter{
		Name:   "message_proxy_success",
		Title:  "Message Proxied Successfully",
		Units:  "message/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.MessageProxySuccess)

	r.MessageProxyFailure = &gangliamr.Meter{
		Name:   "message_proxy_failure",
		Title:  "Message Proxy Failure",
		Units:  "message/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.MessageProxyFailure)

	r.ServerConnHeld = &gangliamr.Timer{
		Name:   "server_conn_held",
		Title:  "Duration a Server Conn was held",
		Groups: gangliaGroup,
	}
	registry.Register(r.ServerConnHeld)

	r.MessageWithResponse = &gangliamr.Meter{
		Name:   "message_with_response",
		Title:  "Message With Response",
		Units:  "message/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.MessageWithResponse)

	r.MessageWithMutation = &gangliamr.Meter{
		Name:   "message_with_mutation",
		Title:  "Message With Mutation",
		Units:  "message/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.MessageWithMutation)

	r.GetLastErrorTimeoutHit = &gangliamr.Meter{
		Name:   "get_last_error_timeout_hit",
		Title:  "getLastError Timeout Hit",
		Units:  "timeout/sec",
		Groups: gangliaGroup,
	}
	registry.Register(r.GetLastErrorTimeoutHit)
}

// Start starts proxies to support this ReplicaSet.
func (r *ReplicaSet) Start() error {
	r.proxyToReal = make(map[string]string)
	r.realToProxy = make(map[string]string)
	r.proxies = make(map[string]*Proxy)

	if r.Addrs == "" {
		return errNoAddrsGiven
	}

	rawAddrs := strings.Split(r.Addrs, ",")
	var err error
	r.lastState, err = r.ReplicaSetStateCreator.FromAddrs(rawAddrs)
	if err != nil {
		return err
	}
	r.restarter = new(sync.Once)

	for _, addr := range r.lastState.Addrs() {
		listener, err := r.newListener()
		if err != nil {
			return err
		}

		p := &Proxy{
			Log:            r.Log,
			ReplicaSet:     r,
			ClientListener: listener,
			ProxyAddr:      r.proxyAddr(listener),
			MongoAddr:      addr,
		}
		if err := r.add(p); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(r.proxies))
	errch := make(chan error, len(r.proxies))
	for _, p := range r.proxies {
		go func(p *Proxy) {
			defer wg.Done()
			if err := p.Start(); err != nil {
				r.Log.Error(err)
				errch <- stackerr.Wrap(err)
			}
		}(p)
	}
	wg.Wait()
	select {
	default:
		return nil
	case err := <-errch:
		return err
	}
}

// Stop stops all the associated proxies for this ReplicaSet.
func (r *ReplicaSet) Stop() error {
	return r.stop(false)
}

func (r *ReplicaSet) stop(hard bool) error {
	var wg sync.WaitGroup
	wg.Add(len(r.proxies))
	errch := make(chan error, len(r.proxies))
	for _, p := range r.proxies {
		go func(p *Proxy) {
			defer wg.Done()
			if err := p.stop(hard); err != nil {
				r.Log.Error(err)
				errch <- stackerr.Wrap(err)
			}
		}(p)
	}
	wg.Wait()
	select {
	default:
		return nil
	case err := <-errch:
		return err
	}
}

// Restart stops all the proxies and restarts them. This is used when we detect
// an RS config change, like when an election happens.
func (r *ReplicaSet) Restart() {
	r.restarter.Do(func() {
		r.Log.Info("restart triggered")
		if err := r.stop(true); err != nil {
			// We log and ignore this hoping for a successful start anyways.
			r.Log.Errorf("stop failed for restart: %s", err)
		} else {
			r.Log.Info("successfully stopped for restart")
		}

		if err := r.Start(); err != nil {
			// We panic here because we can't repair from here and are pretty much
			// fucked.
			panic(fmt.Errorf("start failed for restart: %s", err))
		}

		r.Log.Info("successfully restarted")
	})
}

func (r *ReplicaSet) proxyAddr(l net.Listener) string {
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s:%s", r.proxyHostname(), port)
}

func (r *ReplicaSet) proxyHostname() string {
	const home = "127.0.0.1"

	hostname, err := os.Hostname()
	if err != nil {
		r.Log.Error(err)
		return home
	}

	// The follow logic ensures that the hostname resolves to a local address.
	// If it doesn't we don't use it since it probably wont work anyways.
	hostnameAddrs, err := net.LookupHost(hostname)
	if err != nil {
		r.Log.Error(err)
		return home
	}

	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		r.Log.Error(err)
		return home
	}

	for _, ia := range interfaceAddrs {
		sa := ia.String()
		for _, ha := range hostnameAddrs {
			if sa == ha {
				return hostname
			}
		}
	}
	r.Log.Warnf("hostname %s doesn't resolve to the current host", hostname)
	return home
}

func (r *ReplicaSet) newListener() (net.Listener, error) {
	for i := r.PortStart; i <= r.PortEnd; i++ {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", i))
		if err == nil {
			return listener, nil
		}
	}
	return nil, fmt.Errorf(
		"could not find a free port in range %d-%d",
		r.PortStart,
		r.PortEnd,
	)
}

// add a proxy/mongo mapping.
func (r *ReplicaSet) add(p *Proxy) error {
	if _, ok := r.proxyToReal[p.ProxyAddr]; ok {
		return fmt.Errorf("proxy %s already used in ReplicaSet", p.ProxyAddr)
	}
	if _, ok := r.realToProxy[p.MongoAddr]; ok {
		return fmt.Errorf("mongo %s already exists in ReplicaSet", p.MongoAddr)
	}
	r.Log.Infof("added %s", p)
	r.proxyToReal[p.ProxyAddr] = p.MongoAddr
	r.realToProxy[p.MongoAddr] = p.ProxyAddr
	r.proxies[p.ProxyAddr] = p
	return nil
}

// Proxy returns the corresponding proxy address for the given real mongo
// address.
func (r *ReplicaSet) Proxy(h string) (string, error) {
	p, ok := r.realToProxy[h]
	if !ok {
		return "", fmt.Errorf("mongo %s is not in ReplicaSet", h)
	}
	return p, nil
}

// ProxyMembers returns the list of proxy members in this ReplicaSet.
func (r *ReplicaSet) ProxyMembers() []string {
	members := make([]string, 0, len(r.proxyToReal))
	for r := range r.proxyToReal {
		members = append(members, r)
	}
	return members
}

// SameRS checks if the given replSetGetStatusResponse is the same as the last
// state.
func (r *ReplicaSet) SameRS(o *replSetGetStatusResponse) bool {
	return r.lastState.SameRS(o)
}

// SameIM checks if the given isMasterResponse is the same as the last state.
func (r *ReplicaSet) SameIM(o *isMasterResponse) bool {
	return r.lastState.SameIM(o)
}
