package metric

import (
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/telegraf"
)

// NewSeriesGrouper returns a type that can be used to group fields by series
// and time, so that fields which share these values will be combined into a
// single telegraf.Metric.
//
// This is useful to build telegraf.Metric's when all fields for a series are
// not available at once.
//
// ex:
// - cpu,host=localhost usage_time=42
// - cpu,host=localhost idle_time=42
// + cpu,host=localhost idle_time=42,usage_time=42
func NewSeriesGrouper() *SeriesGrouper {
	return &SeriesGrouper{
		metrics: make(map[uint64]telegraf.Metric),
		ordered: []telegraf.Metric{},
	}
}

type SeriesGrouper struct {
	metrics map[uint64]telegraf.Metric
	ordered []telegraf.Metric
}

// AddMetric adds the given metric to the grouper. If another metric with the same name+tags+timestamp exists, this
// metric will be merged into that one.
// If the metric is not merged with an existing one, the provided metric will be same instance as returned by Metrics().
func (g *SeriesGrouper) AddMetric(m telegraf.Metric) {
	h := fnv.New64a()
	h.Write([]byte(m.Name() + "\n"))
	for _, t := range m.TagList() {
		h.Write([]byte(t.Key + "\n" + t.Value + "\n"))
	}
	ts, _ := m.Time().MarshalBinary()
	h.Write(ts)
	id := h.Sum64()

	if metric, ok := g.metrics[id]; ok {
		for _, f := range m.FieldList() {
			metric.AddField(f.Key, f.Value)
		}
	} else {
		g.metrics[id] = m
		g.ordered = append(g.ordered, m)
	}
}

// AddField adds a field key and value to the series.
func (g *SeriesGrouper) AddField(
	measurement string,
	tags map[string]string,
	tm time.Time,
	field string,
	fieldValue interface{},
) error {
	var err error
	id := groupID(measurement, tags, tm)
	metric := g.metrics[id]
	if metric == nil {
		metric, err = New(measurement, tags, map[string]interface{}{field: fieldValue}, tm)
		if err != nil {
			return err
		}
		g.metrics[id] = metric
		g.ordered = append(g.ordered, metric)
	} else {
		metric.AddField(field, fieldValue)
	}
	return nil
}

// Metrics returns the metrics grouped by series and time.
func (g *SeriesGrouper) Metrics() []telegraf.Metric {
	return g.ordered
}

func groupID(measurement string, tags map[string]string, tm time.Time) uint64 {
	h := fnv.New64a()
	h.Write([]byte(measurement))
	h.Write([]byte("\n"))

	taglist := make([]*telegraf.Tag, 0, len(tags))
	for k, v := range tags {
		taglist = append(taglist,
			&telegraf.Tag{Key: k, Value: v})
	}
	sort.Slice(taglist, func(i, j int) bool { return taglist[i].Key < taglist[j].Key })
	for _, tag := range taglist {
		h.Write([]byte(tag.Key))
		h.Write([]byte("\n"))
		h.Write([]byte(tag.Value))
		h.Write([]byte("\n"))
	}
	h.Write([]byte("\n"))

	io.WriteString(h, strconv.FormatInt(tm.UnixNano(), 10))
	return h.Sum64()
}
