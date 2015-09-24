// Package addrs provides flags to define one or an array of net.Addr.
package addrs

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"strings"
)

type flagOne struct {
	addr *net.Addr
}

func (f *flagOne) Set(addr string) error {
	a, err := resolveAddr(addr)
	if err != nil {
		return err
	}
	*f.addr = a
	return nil
}

func (f *flagOne) String() string {
	if f.addr == nil || (*f.addr) == nil {
		return ""
	}
	return (*f.addr).Network() + ":" + (*f.addr).String()
}

// FlagOneVar sets a single net.Addr by a flag. The value is expected to be a
// colon separated net:host:port.
func FlagOneVar(dest *net.Addr, name string, addr string, usage string) {
	f := &flagOne{addr: dest}
	if addr != "" {
		if err := f.Set(addr); err != nil {
			panic(err)
		}
	}
	flag.Var(f, name, usage)
}

type flagMany struct {
	addrs *[]net.Addr
}

func (f *flagMany) Set(val string) error {
	var resolved []net.Addr
	for _, addr := range strings.Split(val, ",") {
		a, err := resolveAddr(addr)
		if err != nil {
			return err
		}
		resolved = append(resolved, a)
	}
	*f.addrs = resolved
	return nil
}

func (f *flagMany) String() string {
	if f.addrs == nil || (*f.addrs) == nil {
		return ""
	}
	var buf bytes.Buffer
	first := true
	for _, a := range *f.addrs {
		if !first {
			buf.WriteString(",")
		}
		first = false
		buf.WriteString(a.Network())
		buf.WriteString(":")
		buf.WriteString(a.String())
	}
	return buf.String()
}

// FlagManyVar sets a slice of net.Addr by a flag. The values are expected to
// be comma separated list of net:host:port triples.
func FlagManyVar(dest *[]net.Addr, name string, addrs string, usage string) {
	f := &flagMany{addrs: dest}
	if addrs != "" {
		if err := f.Set(addrs); err != nil {
			panic(err)
		}
	}
	flag.Var(f, name, usage)
}

func resolveAddr(addr string) (net.Addr, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf(
			`invalid address format, must be "net:host:port": %s`,
			addr,
		)
	}

	hp := parts[1] + ":" + parts[2]
	switch parts[0] {
	default:
		return nil, net.UnknownNetworkError(parts[0])
	case "ip", "ip4", "ip6":
		return net.ResolveIPAddr(parts[0], hp)
	case "tcp", "tcp4", "tcp6":
		return net.ResolveTCPAddr(parts[0], hp)
	case "udp", "udp4", "udp6":
		return net.ResolveUDPAddr(parts[0], hp)
	case "unix", "unixgram", "unixpacket":
		return net.ResolveUnixAddr(parts[0], hp)
	}
}
