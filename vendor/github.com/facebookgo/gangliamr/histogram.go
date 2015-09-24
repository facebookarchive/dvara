package gangliamr

import (
	"github.com/facebookgo/ganglia/gmetric"
	"github.com/facebookgo/metrics"
)

var histogramTrackedPercentiles = []float64{0.5, 0.75, 0.95, 0.98, 0.99, 0.999}

// Histogram calculates distribution statistics from an int64 value.
type Histogram struct {
	// Unless explicitly specified, this will be a histogram with an
	// exponentially-decaying sample with the same reservoir size and alpha as
	// UNIX load averages.
	metrics.Histogram

	Name        string // Required.
	Title       string
	Units       string // Default is "value".
	Description string
	Groups      []string

	max    gmetric.Metric
	mean   gmetric.Metric
	min    gmetric.Metric
	stddev gmetric.Metric
	p50    gmetric.Metric
	p75    gmetric.Metric
	p95    gmetric.Metric
	p98    gmetric.Metric
	p99    gmetric.Metric
	p999   gmetric.Metric
}

func (h *Histogram) name() string {
	return h.Name
}

func (h *Histogram) writeValue(c *gmetric.Client) {
	ps := h.Percentiles(histogramTrackedPercentiles)
	c.WriteValue(&h.max, h.Max())
	c.WriteValue(&h.mean, h.Mean())
	c.WriteValue(&h.min, h.Min())
	c.WriteValue(&h.stddev, h.StdDev())
	c.WriteValue(&h.p50, ps[0])
	c.WriteValue(&h.p75, ps[1])
	c.WriteValue(&h.p95, ps[2])
	c.WriteValue(&h.p98, ps[3])
	c.WriteValue(&h.p99, ps[4])
	c.WriteValue(&h.p999, ps[5])
}

func (h *Histogram) writeMeta(c *gmetric.Client) {
	c.WriteMeta(&h.max)
	c.WriteMeta(&h.mean)
	c.WriteMeta(&h.min)
	c.WriteMeta(&h.stddev)
	c.WriteMeta(&h.p50)
	c.WriteMeta(&h.p75)
	c.WriteMeta(&h.p95)
	c.WriteMeta(&h.p98)
	c.WriteMeta(&h.p99)
	c.WriteMeta(&h.p999)
}

func (h *Histogram) register(r *Registry) {
	if h.Histogram == nil {
		h.Histogram = metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
	}
	h.max = gmetric.Metric{
		Name:        r.makeName(h.Name, "max"),
		Title:       makeOptional(h.Title, "maximum"),
		Description: makeOptional(h.Description, "maximum"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueUint32,
		Slope:       gmetric.SlopeBoth,
	}
	h.mean = gmetric.Metric{
		Name:        r.makeName(h.Name, "mean"),
		Title:       makeOptional(h.Title, "mean"),
		Description: makeOptional(h.Description, "mean"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueUint32,
		Slope:       gmetric.SlopeBoth,
	}
	h.min = gmetric.Metric{
		Name:        r.makeName(h.Name, "min"),
		Title:       makeOptional(h.Title, "minimum"),
		Description: makeOptional(h.Description, "minimum"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueUint32,
		Slope:       gmetric.SlopeBoth,
	}
	h.stddev = gmetric.Metric{
		Name:        r.makeName(h.Name, "stddev"),
		Title:       makeOptional(h.Title, "standard deviation"),
		Description: makeOptional(h.Description, "standard deviation"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueUint32,
		Slope:       gmetric.SlopeBoth,
	}
	h.p50 = gmetric.Metric{
		Name:        r.makeName(h.Name, "p50"),
		Title:       makeOptional(h.Title, "50th percentile"),
		Description: makeOptional(h.Description, "50th percentile"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	h.p75 = gmetric.Metric{
		Name:        r.makeName(h.Name, "p75"),
		Title:       makeOptional(h.Title, "75th percentile"),
		Description: makeOptional(h.Description, "75th percentile"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	h.p95 = gmetric.Metric{
		Name:        r.makeName(h.Name, "p95"),
		Title:       makeOptional(h.Title, "95th percentile"),
		Description: makeOptional(h.Description, "95th percentile"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	h.p98 = gmetric.Metric{
		Name:        r.makeName(h.Name, "p98"),
		Title:       makeOptional(h.Title, "98th percentile"),
		Description: makeOptional(h.Description, "98th percentile"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	h.p99 = gmetric.Metric{
		Name:        r.makeName(h.Name, "p99"),
		Title:       makeOptional(h.Title, "99th percentile"),
		Description: makeOptional(h.Description, "99th percentile"),
		Units:       nonEmpty(h.Units, "value"),
		Groups:      h.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	h.p999 = gmetric.Metric{
		Name:      r.makeName(h.Name, "p999"),
		Title:     makeOptional(h.Title, "99.9th percentile"),
		Units:     nonEmpty(h.Units, "value"),
		Groups:    h.Groups,
		ValueType: gmetric.ValueFloat64,
		Slope:     gmetric.SlopeBoth,
	}
}
