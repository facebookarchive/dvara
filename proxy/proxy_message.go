package proxy

import (
	"bytes"
	"flag"
	"io"

	"github.com/mcuadros/exmongodb/protocol"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/mgo.v2/bson"
)

var (
	proxyAllQueries = flag.Bool(
		"dvara.proxy-all",
		false,
		"if true all queries will be proxied and logger",
	)

	adminCollectionName = []byte("admin.$cmd\000")
	cmdCollectionSuffix = []byte(".$cmd\000")
)

// ProxyMessage proxies an protocol.OpQuery and a corresponding response.
type ProxyMessage struct {
	Log                              Logger                            `inject:""`
	GetLastErrorRewriter             *GetLastErrorRewriter             `inject:""`
	IsMasterResponseRewriter         *IsMasterResponseRewriter         `inject:""`
	ReplSetGetStatusResponseRewriter *ReplSetGetStatusResponseRewriter `inject:""`
	Extension                        Extension                         `inject:""`
}

// proxyMessage proxies a message, possibly it's response, and possibly a
// follow up call.
func (p *ProxyMessage) Proxy(
	h *protocol.MessageHeader,
	client io.ReadWriter,
	server io.ReadWriter,
	lastError *protocol.LastError,
) error {
	if p.Extension != nil {
		p.Extension.HandleOp(h, client, server, lastError)
	}

	// protocol.OpQuery may need to be transformed and need special handling in order to
	// make the proxy transparent.
	if h.OpCode == protocol.OpQuery {
		//	stats.BumpSum(p.stats, "message.with.response", 1)
		return p.proxyQuery(h, client, server, lastError)
	}

	// Anything besides a getlasterror call (which requires an protocol.OpQuery) resets
	// the lastError.
	if lastError.Exists() {
		p.Log.Debug("reset getLastError cache")
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
		//stats.BumpSum(p.stats, "message.with.response", 1)
		if err := protocol.CopyMessage(client, server); err != nil {
			p.Log.Error(err)
			return err
		}
	}

	return nil
}

// Proxy proxies an OpQuery and a corresponding response.
func (p *ProxyMessage) proxyQuery(
	h *protocol.MessageHeader,
	client io.ReadWriter,
	server io.ReadWriter,
	lastError *protocol.LastError,
) error {
	// https://github.com/mongodb/mongo/search?q=lastError.disableForCommand
	// Shows the logic we need to be in sync with. Unfortunately it isn't a
	// simple check to determine this, and may change underneath us at the mongo
	// layer.
	resetLastError := true

	parts := [][]byte{h.ToWire()}

	var flags [4]byte
	if _, err := io.ReadFull(client, flags[:]); err != nil {
		p.Log.Error(err)
		return err
	}
	parts = append(parts, flags[:])

	fullCollectionName, err := protocol.ReadCString(client)
	if err != nil {
		p.Log.Error(err)
		return err
	}
	parts = append(parts, fullCollectionName)

	var rewriter responseRewriter
	if *proxyAllQueries || bytes.HasSuffix(fullCollectionName, cmdCollectionSuffix) {
		var twoInt32 [8]byte
		if _, err := io.ReadFull(client, twoInt32[:]); err != nil {
			p.Log.Error(err)
			return err
		}
		parts = append(parts, twoInt32[:])

		queryDoc, err := protocol.ReadDocument(client)
		if err != nil {
			p.Log.Error(err)
			return err
		}
		parts = append(parts, queryDoc)

		var q bson.D
		if err := bson.Unmarshal(queryDoc, &q); err != nil {
			p.Log.Error(err)
			return err
		}

		if p.Extension != nil {
			p.Extension.HandleBSON(&q, client, server, lastError)
		}

		p.Log.Debugf(
			"buffered protocol.OpQuery for %s: %s",
			fullCollectionName[:len(fullCollectionName)-1],
			spew.Sdump(q),
		)

		if hasKey(q, "getLastError") {
			return p.GetLastErrorRewriter.Rewrite(
				h,
				parts,
				client,
				server,
				lastError,
			)
		}

		if hasKey(q, "isMaster") {
			rewriter = p.IsMasterResponseRewriter
		}

		if bytes.Equal(adminCollectionName, fullCollectionName) && hasKey(q, "replSetGetStatus") {
			rewriter = p.ReplSetGetStatusResponseRewriter
		}

		if rewriter != nil {
			// If forShell is specified, we don't want to reset the last error. See
			// comment above around resetLastError for details.
			resetLastError = hasKey(q, "forShell")
		}
	}

	if resetLastError && lastError.Exists() {
		p.Log.Debug("reset getLastError cache")
		lastError.Reset()
	}

	var written int
	for _, b := range parts {
		n, err := server.Write(b)
		if err != nil {
			p.Log.Error(err)
			return err
		}
		written += n
	}

	pending := int64(h.MessageLength) - int64(written)
	if _, err := io.CopyN(server, client, pending); err != nil {
		p.Log.Error(err)
		return err
	}

	if rewriter != nil {
		if err := rewriter.Rewrite(client, server); err != nil {
			return err
		}

		return nil
	}

	if err := protocol.CopyMessage(client, server); err != nil {
		p.Log.Error(err)
		return err
	}

	return nil
}
