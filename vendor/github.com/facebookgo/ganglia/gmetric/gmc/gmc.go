// Package gmc is a light weight clone of the gmetric CLI. It only provides a
// subset of the functionality provided by the official cli.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/facebookgo/ganglia/gmetric"
)

func main() {
	hostname, _ := os.Hostname()
	client := gmetric.ClientFromFlag("ganglia")
	value := flag.String("value", "", "Value of the metric")
	groups := flag.String("group", "", "Group(s) of the metric (comma-separated)")
	metric := &gmetric.Metric{}
	flag.StringVar(&metric.Name, "name", "", "Name of the metric")
	flag.StringVar(&metric.Title, "title", "", "Title of the metric")
	flag.StringVar(&metric.Host, "host", hostname, "Hostname")
	flag.StringVar((*string)(&metric.ValueType), "type", "", "Either string|int8|uint8|int16|uint16|int32|uint32|float|double")
	flag.StringVar(&metric.Units, "units", "", "Unit of measure for the value e.g. Kilobytes, Celcius")
	flag.StringVar((*string)(&metric.Slope), "slope", "both", "Either zero|positive|negative|both")
	flag.DurationVar(&metric.TickInterval, "tmax", 60*time.Second, "The maximum time between gmetric calls")
	flag.DurationVar(&metric.Lifetime, "dmax", 0, "The lifetime in seconds of this metric")
	flag.StringVar(&metric.Description, "desc", "", "Description of the metric")
	flag.StringVar(&metric.Spoof, "spoof", "", "IP address and name of host/device (colon separated) we are spoofing")
	flag.Parse()

	if metric.Name == "" || metric.ValueType == "" || *value == "" {
		fmt.Fprintln(os.Stderr, "name, type and value are required")
		flag.Usage()
		os.Exit(2)
	}

	if *groups != "" {
		metric.Groups = strings.Split(*groups, ",")
	}

	if err := client.Open(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := client.WriteMeta(metric); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := client.WriteValue(metric, *value); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := client.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
