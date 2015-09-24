// Package gangliamr provides metrics backed by Ganglia.
//
// The underlying in-memory metrics are used from:
// http://godoc.org/github.com/facebookgo/metrics. Application code should by
// typed to the interfaces defined in that package in order to not be Ganglia
// specific.
//
// The underlying Ganglia library is:
// http://godoc.org/github.com/facebookgo/ganglia/gmetric.
//
// A handful of metrics types are provided, and they all have a similar form.
// The "Name" property is always required, all other properties are optional.
// The metric instances are initialized upon registration and thus the metrics
// must be registered before use.
//
// The common set of properties for the metrics are:
//
//     // The name is used as the file name, and also the title unless one is
//     // explicitly provided. This property is required.
//     Name string
//
//     // The title is for human consumption and is shown atop the graph.
//     Title string
//
//     // The units are shown in the graph to provide context to the numbers.
//     // The default value varies based on the metric type.
//     Units string
//
//     // Descriptions serve as documentation.
//     Description string
//
//     // The groups ensure your metric is kept alongside sibling metrics.
//     Groups []string
package gangliamr
