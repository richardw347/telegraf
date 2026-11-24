package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/models"
)

func TestRunInputStartupCleanup(t *testing.T) {
	for _, tt := range []struct {
		name        string
		processors  bool
		aggregators bool
		shutdown    bool
		probe       bool
	}{
		{name: "outputs"},
		{name: "processors", processors: true},
		{name: "aggregators", aggregators: true},
		{name: "processors and aggregators", processors: true, aggregators: true},
		{name: "normal shutdown", processors: true, aggregators: true, shutdown: true},
		{name: "probe failure", processors: true, aggregators: true, probe: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			startupErr := errors.New("input startup failed")
			input := &startupBurstInput{ready: make(chan struct{}), done: make(chan struct{})}
			failed := &startupFailingInput{ready: input.ready, err: startupErr}
			if tt.shutdown {
				failed.err = nil
				failed.cancel = cancel
			}
			output := &startupRecordingOutput{}
			ro, err := models.NewRunningOutput(output, &models.OutputConfig{Name: "startup"}, 1000, 1000)
			require.NoError(t, err)

			cfg := config.NewConfig()
			cfg.Agent.RoundInterval = false
			cfg.Agent.FlushInterval = config.Duration(time.Hour)
			cfg.Agent.SkipProcessorsAfterAggregators = new(false)
			cfg.Inputs = []*models.RunningInput{
				models.NewRunningInput(input, &models.InputConfig{Name: "burst"}),
				models.NewRunningInput(failed, &models.InputConfig{Name: "failure"}),
			}
			cfg.Outputs = []*models.RunningOutput{ro}
			if tt.probe {
				input.probeErr = startupErr
				input.cancel = cancel
				cfg.Inputs = cfg.Inputs[:1]
				cfg.Inputs[0].Config.StartupErrorBehavior = "probe"
			}

			var processors []*startupForwardingProcessor
			if tt.processors {
				for range 2 {
					processor := &startupForwardingProcessor{}
					processors = append(processors, processor)
					cfg.Processors = append(cfg.Processors, models.NewRunningProcessor(processor, &models.ProcessorConfig{Name: "startup"}))
				}
			}
			if tt.aggregators {
				cfg.Aggregators = []*models.RunningAggregator{
					models.NewRunningAggregator(&startupCountingAggregator{}, &models.AggregatorConfig{Name: "startup", Period: time.Hour}),
				}
				if tt.processors {
					processor := &startupForwardingProcessor{}
					processors = append(processors, processor)
					cfg.AggProcessors = models.RunningProcessors{
						models.NewRunningProcessor(processor, &models.ProcessorConfig{Name: "startup"}),
					}
				}
			}

			finished := make(chan error, 1)
			go func() { finished <- NewAgent(cfg).Run(ctx) }()
			select {
			case err := <-finished:
				if tt.shutdown || tt.probe {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, startupErr)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("startup cleanup did not finish with a full input channel")
			}

			require.Equal(t, 1, input.stops)
			require.Equal(t, 1, output.closes)
			for _, processor := range processors {
				require.Equal(t, 1, processor.stops)
			}

			// All queued metrics and the metric emitted by Stop must reach the
			// output before Run returns, including the aggregator's final push.
			var values []int64
			var stopMetrics, aggregates int
			for _, m := range output.metrics {
				switch m.Name() {
				case "startup":
					value, ok := m.GetField("value")
					require.True(t, ok)
					values = append(values, value.(int64))
				case "stopping":
					stopMetrics++
				case "aggregate":
					aggregates++
					value, ok := m.GetField("count")
					require.True(t, ok)
					require.Equal(t, int64(102), value)
				default:
					t.Fatalf("unexpected metric %q", m.Name())
				}
			}
			expected := make([]int64, 101)
			for i := range expected {
				expected[i] = int64(i)
			}
			require.Equal(t, expected, values)
			require.Equal(t, 1, stopMetrics)
			if tt.aggregators {
				require.Equal(t, 1, aggregates)
			} else {
				require.Zero(t, aggregates)
			}
		})
	}
}

type startupBurstInput struct {
	acc      telegraf.Accumulator
	ready    chan struct{}
	done     chan struct{}
	stops    int
	probeErr error
	cancel   context.CancelFunc
}

func (*startupBurstInput) SampleConfig() string              { return "" }
func (*startupBurstInput) Gather(telegraf.Accumulator) error { return nil }
func (s *startupBurstInput) Start(acc telegraf.Accumulator) error {
	s.acc = acc
	go func() {
		defer close(s.done)
		for i := range 101 {
			if i == 100 {
				// The unpatched agent has no consumers yet: these first 100
				// metrics fill the channel and the next write blocks Stop.
				close(s.ready)
			}
			acc.AddFields("startup", map[string]any{"value": int64(i)}, nil)
		}
	}()
	return nil
}

func (s *startupBurstInput) Stop() {
	s.stops++
	<-s.done
	s.acc.AddFields("stopping", map[string]any{"value": 1}, nil)
}

func (s *startupBurstInput) Probe() error {
	<-s.ready
	s.cancel()
	return s.probeErr
}

type startupFailingInput struct {
	ready  <-chan struct{}
	err    error
	cancel context.CancelFunc
}

func (*startupFailingInput) SampleConfig() string              { return "" }
func (*startupFailingInput) Gather(telegraf.Accumulator) error { return nil }
func (*startupFailingInput) Stop()                             {}
func (s *startupFailingInput) Start(telegraf.Accumulator) error {
	<-s.ready
	if s.cancel != nil {
		s.cancel()
	}
	return s.err
}

type startupRecordingOutput struct {
	metrics []telegraf.Metric
	closes  int
}

func (*startupRecordingOutput) SampleConfig() string { return "" }
func (*startupRecordingOutput) Connect() error       { return nil }
func (s *startupRecordingOutput) Close() error       { s.closes++; return nil }
func (s *startupRecordingOutput) Write(metrics []telegraf.Metric) error {
	s.metrics = append(s.metrics, metrics...)
	return nil
}

type startupForwardingProcessor struct {
	stops int
}

func (*startupForwardingProcessor) SampleConfig() string             { return "" }
func (*startupForwardingProcessor) Start(telegraf.Accumulator) error { return nil }
func (s *startupForwardingProcessor) Stop()                          { s.stops++ }
func (*startupForwardingProcessor) Add(m telegraf.Metric, acc telegraf.Accumulator) error {
	acc.AddMetric(m)
	return nil
}

type startupCountingAggregator struct {
	count int64
}

func (*startupCountingAggregator) SampleConfig() string  { return "" }
func (s *startupCountingAggregator) Add(telegraf.Metric) { s.count++ }
func (s *startupCountingAggregator) Reset()              { s.count = 0 }
func (s *startupCountingAggregator) Push(acc telegraf.Accumulator) {
	if s.count > 0 {
		acc.AddFields("aggregate", map[string]any{"count": s.count}, nil)
	}
}
