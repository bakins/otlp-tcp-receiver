package otlptcp

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/akutz/memconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.5.0"

	"github.com/bakins/otlptcpreceiver/internal/sharedcomponent"
)

func TestTraces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	cfg := &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
		ListenAddress:    "127.0.0.1:0",
		ListenNetwork:    "tcp",
	}

	var sink consumertest.TracesSink

	w := traceWrapper{
		Traces: &sink,
		done:   make(chan struct{}),
	}

	tr, err := NewFactory().CreateTracesReceiver(
		ctx,
		componenttest.NewNopReceiverCreateSettings(),
		cfg,
		&w)
	require.NoError(t, err)

	require.NoError(t, tr.Start(ctx, componenttest.NewNopHost()))

	defer func() {
		assert.NoError(t, tr.Shutdown(ctx))
	}()

	marshaller := ptrace.NewProtoMarshaler()

	data, err := marshaller.MarshalTraces(traceOtlp)
	require.NoError(t, err)

	otlp := tr.(*sharedcomponent.SharedComponent).Unwrap().(*otlpReceiver)

	conn, err := memconn.Dial("tcp", otlp.listener.Addr().String())
	require.NoError(t, err)

	prefix := []byte{MessageTypeTrace, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(data)))

	_, err = conn.Write(prefix)
	require.NoError(t, err)

	_, err = conn.Write(data)
	require.NoError(t, err)

	select {
	case <-w.done:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}

	require.Equal(t, 1, sink.SpanCount())
}

type traceWrapper struct {
	done chan struct{}
	consumer.Traces
}

func (w *traceWrapper) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	select {
	case <-w.done:
	default:
		close(w.done)
	}

	return w.Traces.ConsumeTraces(ctx, td)
}

var traceOtlp = func() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutString(semconv.AttributeHostName, "testHost")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	span1 := spans.AppendEmpty()
	span1.SetTraceID([16]byte{0x5B, 0x8E, 0xFF, 0xF7, 0x98, 0x3, 0x81, 0x3, 0xD2, 0x69, 0xB6, 0x33, 0x81, 0x3F, 0xC6, 0xC})
	span1.SetSpanID([8]byte{0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x74})
	span1.SetParentSpanID([8]byte{0xEE, 0xE1, 0x9B, 0x7E, 0xC3, 0xC1, 0xB1, 0x73})
	span1.SetName("testSpan")
	span1.SetStartTimestamp(1544712660300000000)
	span1.SetEndTimestamp(1544712660600000000)
	span1.SetKind(ptrace.SpanKindServer)
	span1.Attributes().PutInt("attr1", 55)
	return td
}()
