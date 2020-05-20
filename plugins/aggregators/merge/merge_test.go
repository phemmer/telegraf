package seriesgrouper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"
)

func TestSimple(t *testing.T) {
	plugin := &Merge{}

	err := plugin.Init()
	require.NoError(t, err)

	plugin.Add(
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_idle": 42,
			},
			time.Unix(0, 0),
		),
	)
	require.NoError(t, err)

	plugin.Add(
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_guest": 42,
			},
			time.Unix(0, 0),
		),
	)
	require.NoError(t, err)

	var acc testutil.Accumulator
	plugin.Push(&acc)

	expected := []telegraf.Metric{
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_idle":  42,
				"time_guest": 42,
			},
			time.Unix(0, 0),
		),
	}

	testutil.RequireMetricsEqual(t, expected, acc.GetTelegrafMetrics())
}

func TestNanosecondPrecision(t *testing.T) {
	plugin := &Merge{}

	err := plugin.Init()
	require.NoError(t, err)

	plugin.Add(
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_idle": 42,
			},
			time.Unix(0, 1),
		),
	)
	require.NoError(t, err)

	plugin.Add(
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_guest": 42,
			},
			time.Unix(0, 1),
		),
	)
	require.NoError(t, err)

	var acc testutil.Accumulator
	acc.SetPrecision(time.Second)
	plugin.Push(&acc)

	expected := []telegraf.Metric{
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_idle":  42,
				"time_guest": 42,
			},
			time.Unix(0, 1),
		),
	}

	testutil.RequireMetricsEqual(t, expected, acc.GetTelegrafMetrics())
}

func TestHoldTime(t *testing.T) {
	for {
		// Since this test relies on elapsed wall clock time, it is inherently racy.
		// I don't want to put some absurdly high HoldTime which will slow down the tests. So we use a sane value and retry
		// the test if the timing checks fail.

		plugin := &Merge{
			HoldTime: time.Millisecond,
		}

		err := plugin.Init()
		require.NoError(t, err)

		plugin.Add(
			testutil.MustMetric(
				"cpu",
				map[string]string{
					"cpu": "cpu0",
				},
				map[string]interface{}{
					"time_idle": 42,
				},
				time.Unix(0, 0),
			),
		)

		time.Sleep(plugin.HoldTime)

		tStart := time.Now()

		// add a metric which does not merge with the first (the timestamp differs)
		plugin.Add(
			testutil.MustMetric(
				"cpu",
				map[string]string{
					"cpu": "cpu0",
				},
				map[string]interface{}{
					"time_idle": 42,
				},
				time.Unix(0, 1),
			),
		)

		// add a metric which does merge with the first
		plugin.Add(
			testutil.MustMetric(
				"cpu",
				map[string]string{
					"cpu": "cpu0",
				},
				map[string]interface{}{
					"time_user": 53,
				},
				time.Unix(0, 0),
			),
		)

		// Since we slept for plugin.HoldTime, the first point, which has the 3rd merged into it, should emit immediately,
		// while the second point should be held back.
		var acc testutil.Accumulator
		plugin.Push(&acc)
		tStop := time.Now()

		if tStop.Sub(tStart) >= plugin.HoldTime {
			// Took too long. Can't guarantee a valid test. Retry.
			continue
		}

		expected := []telegraf.Metric{
			testutil.MustMetric(
				"cpu",
				map[string]string{
					"cpu": "cpu0",
				},
				map[string]interface{}{
					"time_idle": 42,
					"time_user": 53,
				},
				time.Unix(0,0),
			),
		}
		testutil.RequireMetricsEqual(t, expected, acc.GetTelegrafMetrics())

		// sleep the remaining amount of time to guarantee the second metric is ready.
		time.Sleep(plugin.HoldTime - time.Now().Sub(tStop))

		plugin.Reset()
		plugin.Push(&acc)

		expected = append(expected,
			testutil.MustMetric(
				"cpu",
				map[string]string{
					"cpu": "cpu0",
				},
				map[string]interface{}{
					"time_idle": 42,
				},
				time.Unix(0,1),
			),
		)
		testutil.RequireMetricsEqual(t, expected, acc.GetTelegrafMetrics())

		break
	}
}

func TestReset(t *testing.T) {
	plugin := &Merge{}

	err := plugin.Init()
	require.NoError(t, err)

	plugin.Add(
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_idle": 42,
			},
			time.Unix(0, 0),
		),
	)
	require.NoError(t, err)

	var acc testutil.Accumulator
	plugin.Push(&acc)

	plugin.Reset()

	plugin.Add(
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_guest": 42,
			},
			time.Unix(0, 0),
		),
	)
	require.NoError(t, err)

	plugin.Push(&acc)

	expected := []telegraf.Metric{
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_idle": 42,
			},
			time.Unix(0, 0),
		),
		testutil.MustMetric(
			"cpu",
			map[string]string{
				"cpu": "cpu0",
			},
			map[string]interface{}{
				"time_guest": 42,
			},
			time.Unix(0, 0),
		),
	}

	testutil.RequireMetricsEqual(t, expected, acc.GetTelegrafMetrics())
}
