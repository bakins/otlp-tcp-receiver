package otlptcp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/bakins/otlptcpreceiver/internal/sharedcomponent"
)

const (
	typeStr = "otlptcp"

	defaultAddress = "0.0.0.0:14317"

	// DefaultMaxMessageSize is the max buffer sized used
	// if MaxMessageSize is not set
	DefaultMaxMessageSize = 4 * 1024 * 1024
)

const (
	MessageTypeUnset uint8 = iota
	MessageTypeTrace
	MessageTypeMetric
	MessageTypeLog
)

// NewFactory creates a new OTLP TCP receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, component.StabilityLevelStable),
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelStable),
		receiver.WithLogs(createLogReceiver, component.StabilityLevelBeta),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ListenAddress:  defaultAddress,
		MaxMessageSize: DefaultMaxMessageSize,
	}
}

// Config defines configuration for OTLP TCP receiver.
type Config struct {
	ListenAddress  string          `mapstructure:"listen_address,omitempty"`
	MaxMessageSize helper.ByteSize `mapstructure:"max_message_size,omitempty"`
}

func createTracesReceiver(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.GetOrAdd(oCfg, func() (*otlpReceiver, error) {
		return newOtlpReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}

	r.Unwrap().nextTraceConsumer = nextConsumer

	return r, nil
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.GetOrAdd(oCfg, func() (*otlpReceiver, error) {
		return newOtlpReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}

	r.Unwrap().nextMetricConsumer = consumer

	return r, nil
}

func createLogReceiver(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.GetOrAdd(oCfg, func() (*otlpReceiver, error) {
		return newOtlpReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}

	r.Unwrap().nextLogConsumer = consumer

	return r, nil
}

type otlpReceiver struct {
	waitgroup          sync.WaitGroup
	listener           net.Listener
	nextTraceConsumer  consumer.Traces
	nextMetricConsumer consumer.Metrics
	nextLogConsumer    consumer.Logs
	cfg                *Config
	logger             *zap.Logger
	cancel             context.CancelFunc
	settings           receiver.CreateSettings
	backoff            backoff.Backoff
}

func newOtlpReceiver(cfg *Config, settings receiver.CreateSettings) (*otlpReceiver, error) {
	logger := settings.TelemetrySettings.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	logger = logger.With(zap.String("component", typeStr))

	r := &otlpReceiver{
		cfg:      cfg,
		settings: settings,
		backoff: backoff.Backoff{
			Max: time.Second,
		},
		logger: logger,
	}

	return r, nil
}

func (r *otlpReceiver) Start(_ context.Context, host component.Host) error {
	listener, err := net.Listen("tcp", r.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to configure tcp listener: %w", err)
	}

	r.listener = listener
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.goListen(ctx)

	return nil
}

func (r *otlpReceiver) goListen(ctx context.Context) {
	r.waitgroup.Add(1)

	go func() {
		defer r.waitgroup.Done()

		for {
			conn, err := r.listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(r.backoff.Duration())
					continue
				}
			}

			r.backoff.Reset()

			subctx, cancel := context.WithCancel(ctx)
			r.goHandleClose(subctx, conn)
			r.goHandleMessages(subctx, conn, cancel)
		}
	}()
}

func (r *otlpReceiver) goHandleClose(ctx context.Context, conn net.Conn) {
	r.waitgroup.Add(1)

	go func() {
		defer r.waitgroup.Done()
		<-ctx.Done()
		if err := conn.Close(); err != nil {
			// TODO: log error
			_ = err
		}
	}()
}

func (r *otlpReceiver) goHandleMessages(ctx context.Context, conn net.Conn, cancel context.CancelFunc) {
	r.waitgroup.Add(1)

	bufSize := r.cfg.MaxMessageSize
	if bufSize < 64*1024 {
		bufSize = 64 * 1024
	}

	go func() {
		defer r.waitgroup.Done()
		defer cancel()

		buf := make([]byte, 0, bufSize)

		traceUnmarshaler := &ptrace.ProtoUnmarshaler{}
		metricUnmarshaler := &pmetric.ProtoUnmarshaler{}
		logUnmarshaler := &plog.ProtoUnmarshaler{}

		for {
			prefix := []byte{0, 0, 0, 0, 0}

			if _, err := io.ReadFull(conn, prefix); err != nil && err != net.ErrClosed {
				r.logger.Warn("unable to read message prefix", zap.Error(err))
				return
			}

			messageType := uint8(prefix[0])
			switch messageType {
			case MessageTypeTrace:
				if r.nextTraceConsumer == nil {
					r.logger.Warn("no trace consumer", zap.Error(component.ErrNilNextConsumer))
					return
				}
			case MessageTypeMetric:
				if r.nextMetricConsumer == nil {
					r.logger.Warn("no metric consumer", zap.Error(component.ErrNilNextConsumer))
					return
				}
			case MessageTypeLog:
				if r.nextLogConsumer == nil {
					r.logger.Warn("no log consumer", zap.Error(component.ErrNilNextConsumer))
					return
				}
			default:
				r.logger.Warn("unknown message type", zap.Uint8("type", messageType))
				return
			}

			length := binary.BigEndian.Uint32(prefix[1:])
			if length == 0 {
				r.logger.Warn("zero length message is not allowed")
				return
			}

			if length > uint32(bufSize) {
				r.logger.Warn("message is too large", zap.Uint32("size", length))
				return
			}

			data := buf[0:length]
			if _, err := io.ReadFull(conn, data); err != nil {
				r.logger.Warn("unable to read message", zap.Error(err))
				return
			}

			switch messageType {
			case MessageTypeTrace:
				traces, err := traceUnmarshaler.UnmarshalTraces(data)
				if err != nil {
					r.logger.Warn("unable to unmarshal traces", zap.Error(err))
					continue
				}

				if err := r.nextTraceConsumer.ConsumeTraces(ctx, traces); err != nil {
					r.logger.Warn("unable to send traces to next consumer", zap.Error(err))
				}
			case MessageTypeMetric:
				metrics, err := metricUnmarshaler.UnmarshalMetrics(data)
				if err != nil {
					r.logger.Warn("unable to unmarshal metrics", zap.Error(err))
					continue
				}

				if err := r.nextMetricConsumer.ConsumeMetrics(ctx, metrics); err != nil {
					r.logger.Warn("unable to send metrics to next consumer", zap.Error(err))
				}

			case MessageTypeLog:
				logs, err := logUnmarshaler.UnmarshalLogs(data)
				if err != nil {
					r.logger.Warn("unable to unmarshal logs", zap.Error(err))
					continue
				}

				if err := r.nextLogConsumer.ConsumeLogs(ctx, logs); err != nil {
					r.logger.Warn("unable to send logs to next consumer", zap.Error(err))
				}
			}
		}
	}()
}

// Shutdown is a method to turn off receiving.
func (r *otlpReceiver) Shutdown(ctx context.Context) error {
	r.cancel()

	if r.listener != nil {
		if err := r.listener.Close(); err != nil {
			r.logger.Warn("failed to close TCP listener", zap.Error(err))
		}
	}

	r.waitgroup.Wait()

	return nil
}

// This is the map of already created OTLP receivers for particular configurations.
// We maintain this map because the Factory is asked trace and metric receivers separately
// when it gets CreateTracesReceiver() and CreateMetricsReceiver() but they must not
// create separate objects, they must use one otlpReceiver object per configuration.
// When the receiver is shutdown it should be removed from this map so the same configuration
// can be recreated successfully.
var receivers = sharedcomponent.NewSharedComponents[*Config, *otlpReceiver]()
