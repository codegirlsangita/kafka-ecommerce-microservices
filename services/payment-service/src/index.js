const express = require('express');
const mongoose = require('mongoose');
const { Kafka, logLevel } = require('kafkajs');
require('dotenv').config();

const app = express();
app.use(express.json());

const SERVICE_NAME = 'payment-service';
const PORT = Number(process.env.PAYMENT_SERVICE_PORT || 3003);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const MONGODB_URI = process.env.PAYMENT_MONGODB_URI
  || process.env.MONGODB_URI
  || 'mongodb://admin:password@localhost:27017/payment_db?authSource=admin';
const ORDER_CREATED_TOPIC = process.env.KAFKA_TOPIC_ORDER_CREATED || 'order.created';
const PAYMENT_COMPLETED_TOPIC = process.env.KAFKA_TOPIC_PAYMENT_COMPLETED || 'payment.completed';
const CONSUMER_GROUP_ID = process.env.KAFKA_CONSUMER_GROUP_PAYMENT || 'payment-service-group';

// Initialize Kafka
// This service is both a CONSUMER and PRODUCER
// Consumer: listens to "order.created" topic
// Producer: publishes "payment.completed" or "payment.failed" events
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || SERVICE_NAME,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR, // Reduce log verbosity
});

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
  // Retry configuration for resilience
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
    multiplier: 2,
  },
});

// Consumer Group: 'payment-service-group'
// All instances of payment-service with same groupId share the load (partition distribution)
const consumer = kafka.consumer({
  groupId: CONSUMER_GROUP_ID,
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  heartbeatInterval: 3000,
});

// MongoDB Connection
mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => console.log('Payment Service: MongoDB connected'))
  .catch(err => console.error('MongoDB connection error:', err));

// Payment Schema
const paymentSchema = new mongoose.Schema({
  orderId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  amount: { type: Number, required: true },
  status: {
    type: String,
    enum: ['pending', 'processing', 'completed', 'failed'],
    default: 'pending',
  },
  transactionId: String,
  errorMessage: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

const Payment = mongoose.model('Payment', paymentSchema);

// Simulated payment processing
// In real scenario, this would call payment gateway (Stripe, PayPal, etc.)
async function processPayment(orderId, amount) {
  // Simulate payment processing
  return new Promise((resolve) => {
    setTimeout(() => {
      // 90% success rate for demo
      const success = Math.random() > 0.1;
      if (success) {
        resolve({
          success: true,
          transactionId: `TXN-${Date.now()}`,
        });
      } else {
        resolve({
          success: false,
          error: 'Payment gateway timeout',
        });
      }
    }, 1000);
  });
}

// CONSUMER: Listen to "order.created" topic
// This service processes payments when orders are created
async function startConsumer() {
  try {
    await consumer.subscribe({
      topic: ORDER_CREATED_TOPIC,
      // fromBeginning: true, // Uncomment to reprocess all previous messages
    });

    // Run consumer with error handling and retry logic
    await consumer.run({
      // eachMessage: called for each message in the topic
      eachMessage: async ({ topic, partition, message }) => {
        let retries = 0;
        const maxRetries = 3;

        const processMessage = async () => {
          try {
            const event = JSON.parse(message.value.toString());
            console.log(
              `[PAYMENT SERVICE] Processing order from message:`,
              event,
              `\nPartition: ${partition}, Offset: ${message.offset}`
            );

            const { orderId, userId, totalAmount } = event;

            // Create payment record
            let payment = new Payment({
              orderId,
              userId,
              amount: totalAmount,
              status: 'processing',
            });
            await payment.save();

            // Process payment
            const result = await processPayment(orderId, totalAmount);

            if (result.success) {
              // Update payment status
              payment.status = 'completed';
              payment.transactionId = result.transactionId;
              await payment.save();

              // Publish payment.completed event
              await producer.send({
                topic: PAYMENT_COMPLETED_TOPIC,
                messages: [
                  {
                    key: userId, // Partition by userId
                    value: JSON.stringify({
                      orderId,
                      userId,
                      transactionId: result.transactionId,
                      amount: totalAmount,
                      status: 'completed',
                      timestamp: new Date().toISOString(),
                    }),
                  },
                ],
              });

              console.log(`[PAYMENT SERVICE] Payment completed for order: ${orderId}`);
            } else {
              throw new Error(result.error);
            }
          } catch (error) {
            console.error(
              `[PAYMENT SERVICE] Error processing message (attempt ${retries + 1}/${maxRetries}):`,
              error.message
            );

            retries++;
            if (retries < maxRetries) {
              console.log(`Retrying in 2 seconds...`);
              await new Promise(resolve => setTimeout(resolve, 2000));
              return processMessage();
            } else {
              // After max retries, publish payment.failed event
              const event = JSON.parse(message.value.toString());
              await producer.send({
                topic: PAYMENT_COMPLETED_TOPIC,
                messages: [
                  {
                    key: event.userId,
                    value: JSON.stringify({
                      orderId: event.orderId,
                      userId: event.userId,
                      status: 'failed',
                      errorMessage: error.message,
                      timestamp: new Date().toISOString(),
                    }),
                  },
                ],
              });
              console.error(`[PAYMENT SERVICE] Payment failed after ${maxRetries} retries for order: ${event.orderId}`);
            }
          }
        };

        await processMessage();
      },
    });

    console.log('✓ Payment Service: Consumer listening to order.created topic');
  } catch (error) {
    console.error('Error starting consumer:', error);
  }
}

// Endpoint: Get payment status
app.get('/payments/:orderId', async (req, res) => {
  try {
    const payment = await Payment.findOne({ orderId: req.params.orderId });
    if (!payment) {
      return res.status(404).json({ error: 'Payment not found' });
    }
    res.json(payment);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Endpoint: Get all payments
app.get('/payments', async (req, res) => {
  try {
    const payments = await Payment.find();
    res.json(payments);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/health', async (req, res) => {
  const mongoState = mongoose.connection.readyState;
  const mongoConnected = mongoState === 1;
  res.status(mongoConnected ? 200 : 503).json({
    status: mongoConnected ? 'ok' : 'degraded',
    service: SERVICE_NAME,
    dependencies: {
      mongodb: mongoConnected ? 'up' : 'down',
      kafka: 'up',
    },
  });
});

// Initialize Kafka and start server
async function start() {
  try {
    await producer.connect();
    console.log('✓ Payment Service: Kafka Producer connected');

    await consumer.connect();
    console.log('✓ Payment Service: Kafka Consumer connected');

    await startConsumer();

    app.listen(PORT, () => {
      console.log(`Payment Service running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start Payment Service:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down Payment Service...');
  await producer.disconnect();
  await consumer.disconnect();
  process.exit(0);
});

start();
