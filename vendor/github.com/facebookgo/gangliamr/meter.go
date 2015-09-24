package gangliamr

import (
	"github.com/facebookgo/ganglia/gmetric"
	"github.com/facebookgo/metrics"
)

// Meter counts events to produce exponentially-weighted moving average rates
// at one-, five-, and fifteen-minutes and a mean rate.
type Meter struct {
	metrics.Meter
	Name        string // Required.
	Title       string
	Units       string // Default is "count/sec".
	Description string
	Groups      []string
	count       gmetric.Metric
	m1rate      gmetric.Metric
	m5rate      gmetric.Metric
	m15rate     gmetric.Metric
	meanRate    gmetric.Metric
}

func (m *Meter) name() string {
	return m.Name
}

func (m *Meter) writeMeta(c *gmetric.Client) {
	c.WriteMeta(&m.count)
	c.WriteMeta(&m.m1rate)
	c.WriteMeta(&m.m5rate)
	c.WriteMeta(&m.m15rate)
	c.WriteMeta(&m.meanRate)
}

func (m *Meter) writeValue(c *gmetric.Client) {
	c.WriteValue(&m.count, m.Count())
	c.WriteValue(&m.m1rate, m.Rate1())
	c.WriteValue(&m.m5rate, m.Rate5())
	c.WriteValue(&m.m15rate, m.Rate15())
	c.WriteValue(&m.meanRate, m.RateMean())
}

func (m *Meter) register(r *Registry) {
	const units = "count/sec"
	if m.Meter == nil {
		m.Meter = metrics.NewMeter()
	}
	m.m1rate = gmetric.Metric{
		Name:        r.makeName(m.Name, "one-minute"),
		Title:       makeOptional(m.Title, "one minute"),
		Units:       nonEmpty(m.Units, units),
		Description: makeOptional(m.Description, "one minute"),
		Groups:      m.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	m.m5rate = gmetric.Metric{
		Name:        r.makeName(m.Name, "five-minute"),
		Title:       makeOptional(m.Title, "five minute"),
		Units:       nonEmpty(m.Units, units),
		Description: makeOptional(m.Description, "five minute"),
		Groups:      m.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	m.m15rate = gmetric.Metric{
		Name:        r.makeName(m.Name, "fifteen-minute"),
		Title:       makeOptional(m.Title, "fifteen minute"),
		Units:       nonEmpty(m.Units, units),
		Description: makeOptional(m.Description, "fifteen minute"),
		Groups:      m.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
	m.meanRate = gmetric.Metric{
		Name:        r.makeName(m.Name, "mean"),
		Title:       makeOptional(m.Title, "mean"),
		Units:       nonEmpty(m.Units, units),
		Description: makeOptional(m.Description, "mean"),
		Groups:      m.Groups,
		ValueType:   gmetric.ValueFloat64,
		Slope:       gmetric.SlopeBoth,
	}
}
