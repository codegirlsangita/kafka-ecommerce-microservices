// Winston logger configured to emit structured JSON and to stamp every log line
// with the active OTel trace/span IDs — that's the link that lets you pivot
// from a log line straight to its trace in Jaeger (and back).

const winston = require('winston');
const { trace } = require('@opentelemetry/api');

const SERVICE_NAME = process.env.OTEL_SERVICE_NAME || 'order-service';

const traceContextFormat = winston.format((info) => {
  const span = trace.getActiveSpan();
  if (span) {
    const ctx = span.spanContext();
    info.traceId = ctx.traceId;
    info.spanId = ctx.spanId;
  }
  return info;
});

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  defaultMeta: { service: SERVICE_NAME },
  format: winston.format.combine(
    traceContextFormat(),
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [new winston.transports.Console()],
});

module.exports = logger;
