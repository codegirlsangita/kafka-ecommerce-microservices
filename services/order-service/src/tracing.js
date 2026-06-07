// OpenTelemetry setup — must be required BEFORE any other module (express, mongoose, kafkajs)
// so that auto-instrumentation can patch them at load time.
//
// What this gives us automatically (via auto-instrumentations-node):
//   - Express: a span per incoming HTTP request/route
//   - HTTP: a span per outgoing/incoming HTTP call
//   - MongoDB/Mongoose: a span per database query
//
// Kafka is NOT auto-instrumented well by this package, so we create spans
// manually around producer.send / consumer eachMessage in index.js, and
// propagate trace context through Kafka message headers.

const { NodeSDK } = require('@opentelemetry/sdk-node');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { ATTR_SERVICE_NAME } = require('@opentelemetry/semantic-conventions');

const SERVICE_NAME = process.env.OTEL_SERVICE_NAME || 'order-service';
const OTLP_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318/v1/traces';

const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    [ATTR_SERVICE_NAME]: SERVICE_NAME,
  }),
  traceExporter: new OTLPTraceExporter({ url: OTLP_ENDPOINT }),
  instrumentations: [getNodeAutoInstrumentations()],
});

sdk.start();
console.log(`[${SERVICE_NAME}] OpenTelemetry tracing started — exporting to ${OTLP_ENDPOINT}`);

process.on('SIGINT', () => sdk.shutdown().finally(() => process.exit(0)));
process.on('SIGTERM', () => sdk.shutdown().finally(() => process.exit(0)));
