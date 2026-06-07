// Prometheus metrics via prom-client.
// register collects:
//   - Node.js default metrics (event loop lag, GC, memory, CPU) via collectDefaultMetrics
//   - Our custom HTTP and Kafka metrics defined below
// Exposed at GET /metrics for Prometheus to scrape.

const client = require('prom-client');

const SERVICE_NAME = process.env.OTEL_SERVICE_NAME || 'order-service';

const register = new client.Registry();
register.setDefaultLabels({ service: SERVICE_NAME });
client.collectDefaultMetrics({ register });

const httpRequestDuration = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5],
  registers: [register],
});

const httpRequestsTotal = new client.Counter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code'],
  registers: [register],
});

const kafkaMessagesProducedTotal = new client.Counter({
  name: 'kafka_messages_produced_total',
  help: 'Total number of Kafka messages produced',
  labelNames: ['topic'],
  registers: [register],
});

const kafkaMessagesConsumedTotal = new client.Counter({
  name: 'kafka_messages_consumed_total',
  help: 'Total number of Kafka messages consumed',
  labelNames: ['topic'],
  registers: [register],
});

// Express middleware that times each request and records it under its route
// pattern (e.g. "/orders/:id" rather than "/orders/665f1..."), so requests
// to the same endpoint aggregate together instead of creating one series per ID.
function httpMetricsMiddleware(req, res, next) {
  const stopTimer = httpRequestDuration.startTimer();
  res.on('finish', () => {
    const route = req.route ? `${req.baseUrl}${req.route.path}` : req.path;
    const labels = { method: req.method, route, status_code: res.statusCode };
    stopTimer(labels);
    httpRequestsTotal.inc(labels);
  });
  next();
}

module.exports = {
  register,
  httpMetricsMiddleware,
  kafkaMessagesProducedTotal,
  kafkaMessagesConsumedTotal,
};
