package gangliamr

import (
	"github.com/facebookgo/ganglia/gmetric"
	"github.com/facebookgo/metrics"
)

// Gauge holds an int64 value that can be set arbitrarily.
type Gauge struct {
	metrics.Gauge
	Name        string // Required.
	Title       string
	Units       string // Default is "value".
	Description string
	Groups      []string
	gmetric     gmetric.Metric
}

func (g *Gauge) name() string {
	return g.Name
}

func (g *Gauge) writeMeta(c *gmetric.Client) {
	c.WriteMeta(&g.gmetric)
}

func (g *Gauge) writeValue(c *gmetric.Client) {
	c.WriteValue(&g.gmetric, g.Value())
}

func (g *Gauge) register(r *Registry) {
	if g.Gauge == nil {
		g.Gauge = metrics.NewGauge()
	}
	g.gmetric = gmetric.Metric{
		Name:        r.makeName(g.Name),
		Title:       g.Title,
		Units:       nonEmpty(g.Units, "value"),
		Description: g.Description,
		Groups:      g.Groups,
		ValueType:   gmetric.ValueUint32,
		Slope:       gmetric.SlopeBoth,
	}
}
