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
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

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

	traces := tracepb.TracesData{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								Name: "testing1234",
							},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(&traces)
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
	name := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()
	require.Equal(t, "testing1234", name)
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
