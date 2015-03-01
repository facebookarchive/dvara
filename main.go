package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mcuadros/exmongodb/extensions"
	"github.com/mcuadros/exmongodb/proxy"

	"github.com/facebookgo/gangliamr"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/facebookgo/stats"
)

func main() {
	if err := Main(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Main() error {
	messageTimeout := flag.Duration("message_timeout", 2*time.Minute, "timeout for one message to be proxied")
	clientIdleTimeout := flag.Duration("client_idle_timeout", 60*time.Minute, "idle timeout for client connections")
	getLastErrorTimeout := flag.Duration("get_last_error_timeout", time.Minute, "timeout for getLastError pinning")
	maxConnections := flag.Uint("max_connections", 100, "maximum number of connections per mongo")

	flag.Parse()

	replicaSet := proxy.Proxy{
		Log:                 &stdLogger{},
		ProxyAddr:           "localhost:7000",
		MongoAddr:           "localhost:27017",
		MessageTimeout:      *messageTimeout,
		ClientIdleTimeout:   *clientIdleTimeout,
		GetLastErrorTimeout: *getLastErrorTimeout,
		MaxConnections:      *maxConnections,
		MinIdleConnections:  5,
		ServerIdleTimeout:   5 * time.Minute,
		ServerClosePoolSize: 5,
	}

	var statsClient stats.HookClient
	var log stdLogger
	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &log},
		&inject.Object{Value: &replicaSet},
		&inject.Object{Value: &statsClient},
		&inject.Object{Value: &extensions.DumpExtension{}},
	)
	if err != nil {
		return err
	}
	if err := graph.Populate(); err != nil {
		return err
	}
	objects := graph.Objects()

	// Temporarily setup the metrics against a test registry.
	gregistry := gangliamr.NewTestRegistry()
	for _, o := range objects {
		if rmO, ok := o.Value.(registerMetrics); ok {
			rmO.RegisterMetrics(gregistry)
		}
	}
	if err := startstop.Start(objects, &log); err != nil {
		return err
	}
	defer startstop.Stop(objects, &log)

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	return nil
}

type registerMetrics interface {
	RegisterMetrics(r *gangliamr.Registry)
}
