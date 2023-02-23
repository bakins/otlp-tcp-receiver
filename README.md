# otlptcp receiver

`otlptcp` is a receiver for [opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib) that receives traces, metrics, and logs
via a tcp listener.

## Config

Example:

```yaml
receivers:
  otlptcp:
    listen_address: 0.0.0.0:14317

exporters:
  otlp:
    endpoint: otelcol:4317

service:
  pipelines:
    traces:
      receivers: [otlptcp]
      exporters: [otlp]
    metrics:
      receivers: [otlptcp]
      exporters: [otlp]
    logs:
      receivers: [otlptcp]
      exporters: [otlp]
```

Availible configuration options:

- `listen_address` - listening address in the form of `<ip>:port`. default is `0.0.0.0:14317`
- `max_message_size` - maximum size of a single message. Default to 1m.
- `listen_network` - listent network.  Only `tcp` is supported.


## Usage in collector

See https://opentelemetry.io/docs/collector/custom-collector/

## Protocol

Each message is prefixed with 5 bytes. 

The first byte is an unsigned 8 bit integer that denotes the type of the message. Valid values are:

- 1 - [trace request](https://github.com/open-telemetry/opentelemetry-proto/blob/v0.19.0/opentelemetry/proto/collector/trace/v1/trace_service.proto#L30)
- 2 - [metric request](https://github.com/open-telemetry/opentelemetry-proto/blob/v0.19.0/opentelemetry/proto/collector/metrics/v1/metrics_service.proto#L36)
- 3 - [log request](https://github.com/open-telemetry/opentelemetry-proto/blob/v0.19.0/opentelemetry/proto/collector/logs/v1/logs_service.proto#L36)


The next 4 bytes should be an unsigned 32 bit integer in [big-endian order](https://en.wikipedia.org/wiki/Endianness).

The message is a protocol buffer encoded request of the appropriate message type.

