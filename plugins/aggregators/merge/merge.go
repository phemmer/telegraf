package seriesgrouper

import (
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

const (
	description  = "Merge metrics into multifield metrics by series key"
	sampleConfig = `
  ## If true, the original metric will be dropped by the
  ## aggregator and will not get sent to the output plugins.
  drop_original = true

  # The minimum amount of time a point is held waiting for additional points to merge.
  # Telegraf flushes buffered metrics on a periodic interval. Thus it's possible a point may come in right before a
  # flush is triggered, and another point that would have merged to come in after the flush.'
  # The hold_time effectively puts any points that came in within the configured duration before a flush into the next
  # batch.
  # Note that the hold time is actual wall clock time, and does not use the time on the metric.
  hold_time = 0ms
`
)

type wallTimeMetric struct {
	wallTime time.Time
	telegraf.Metric
}

type Merge struct {
	HoldTime time.Duration `toml:"hold_time"`

	grouper *metric.SeriesGrouper
	nextGrouper *metric.SeriesGrouper
	log     telegraf.Logger
}

func (a *Merge) Init() error {
	a.grouper = metric.NewSeriesGrouper()
	return nil
}

func (a *Merge) Description() string {
	return description
}

func (a *Merge) SampleConfig() string {
	return sampleConfig
}

func (a *Merge) Add(m telegraf.Metric) {
	wtm := wallTimeMetric{time.Now(), m}
	a.grouper.AddMetric(wtm)
}

func (a *Merge) Push(acc telegraf.Accumulator) {
	// Always use nanosecond precision to avoid rounding metrics that were
	// produced at a precision higher than the agent default.
	acc.SetPrecision(time.Nanosecond)

	cutoff := time.Now().Add(-a.HoldTime)
	a.nextGrouper = metric.NewSeriesGrouper()
	for _, m := range a.grouper.Metrics() {
		wtm := m.(wallTimeMetric)
		if wtm.wallTime.After(cutoff) {
			a.nextGrouper.AddMetric(wtm)
		} else {
			acc.AddMetric(wtm.Metric)
		}
	}
}

func (a *Merge) Reset() {
	a.grouper = a.nextGrouper
}

func init() {
	aggregators.Add("merge", func() telegraf.Aggregator {
		return &Merge{}
	})
}
