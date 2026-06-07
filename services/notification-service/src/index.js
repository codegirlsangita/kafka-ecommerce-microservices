const express = require('express');
const mongoose = require('mongoose');
const { Kafka, logLevel } = require('kafkajs');
require('dotenv').config();

const app = express();
app.use(express.json());

const SERVICE_NAME = 'notification-service';
const PORT = Number(process.env.NOTIFICATION_SERVICE_PORT || 3004);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const MONGODB_URI = process.env.NOTIFICATION_MONGODB_URI
  || process.env.MONGODB_URI
  || 'mongodb://admin:password@localhost:27017/notification_db?authSource=admin';
const USER_CREATED_TOPIC = process.env.KAFKA_TOPIC_USER_CREATED || 'user.created';
const PAYMENT_COMPLETED_TOPIC = process.env.KAFKA_TOPIC_PAYMENT_COMPLETED || 'payment.completed';
const CONSUMER_GROUP_ID = process.env.KAFKA_CONSUMER_GROUP_NOTIFICATION || 'notification-service-group';

// Initialize Kafka
// This service is a PURE CONSUMER
// Listens to multiple topics: user.created, payment.completed
// Demonstrates multi-topic subscription pattern
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || SERVICE_NAME,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

// Consumer Group: 'notification-service-group'
// Multiple instances of this service will share the partitions
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
}).then(() => console.log('Notification Service: MongoDB connected'))
  .catch(err => console.error('MongoDB connection error:', err));

// Notification Schema
const notificationSchema = new mongoose.Schema({
  userId: { type: String, required: true },
  type: {
    type: String,
    enum: ['welcome', 'order_confirmation', 'payment_success', 'payment_failed'],
  },
  message: String,
  status: {
    type: String,
    enum: ['pending', 'sent', 'failed'],
    default: 'pending',
  },
  topic: String, // Which Kafka topic this came from
  eventData: mongoose.Schema.Types.Mixed,
  createdAt: { type: Date, default: Date.now },
});

const Notification = mongoose.model('Notification', notificationSchema);

// Simulated email/SMS sender
async function sendNotification(userId, type, message) {
  console.log(`📧 [NOTIFICATION] Sending ${type} to user ${userId}: ${message}`);
  // In real scenario, call email/SMS service like SendGrid, Twilio, etc.
  return Promise.resolve();
}

// Handler for user.created events
async function handleUserCreated(event) {
  try {
    const { userId, name } = event;
    const message = `Welcome ${name}! Your account has been created.`;

    await sendNotification(userId, 'welcome', message);

    const notification = new Notification({
      userId,
      type: 'welcome',
      message,
      topic: USER_CREATED_TOPIC,
      eventData: event,
    });
    await notification.save();

    console.log(`[NOTIFICATION SERVICE] Welcome notification sent for user: ${userId}`);
  } catch (error) {
    console.error('Error handling user.created:', error.message);
  }
}

// Handler for payment.completed events
async function handlePaymentCompleted(event) {
  try {
    const { userId, orderId, status } = event;

    let type, message;
    if (status === 'completed') {
      type = 'payment_success';
      message = `Payment successful for order ${orderId}. Your order is confirmed!`;
    } else {
      type = 'payment_failed';
      message = `Payment failed for order ${orderId}. Please try again.`;
    }

    await sendNotification(userId, type, message);

    const notification = new Notification({
      userId,
      type,
      message,
      topic: PAYMENT_COMPLETED_TOPIC,
      eventData: event,
    });
    await notification.save();

    console.log(`[NOTIFICATION SERVICE] ${type} notification sent for order: ${orderId}`);
  } catch (error) {
    console.error('Error handling payment.completed:', error.message);
  }
}

// CONSUMER: Listen to multiple Kafka topics
async function startConsumer() {
  try {
    // Subscribe to multiple topics using regex or array
    await consumer.subscribe({
      topics: [USER_CREATED_TOPIC, PAYMENT_COMPLETED_TOPIC],
      // fromBeginning: true, // Uncomment to read all previous messages
    });

    // Run consumer
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          console.log(`\n[NOTIFICATION SERVICE] Received ${topic} event:`);
          console.log(`  Event:`, event);
          console.log(`  Partition: ${partition}, Offset: ${message.offset}\n`);

          // Route to appropriate handler based on topic
          if (topic === USER_CREATED_TOPIC) {
            await handleUserCreated(event);
          } else if (topic === PAYMENT_COMPLETED_TOPIC) {
            await handlePaymentCompleted(event);
          }
        } catch (error) {
          console.error('Error processing notification:', error.message);
        }
      },
    });

    console.log('✓ Notification Service: Consumer listening to multiple topics');
  } catch (error) {
    console.error('Error starting consumer:', error);
  }
}

// Endpoint: Get all notifications
app.get('/notifications', async (req, res) => {
  try {
    const notifications = await Notification.find().sort({ createdAt: -1 });
    res.json(notifications);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Endpoint: Get notifications by user
app.get('/notifications/user/:userId', async (req, res) => {
  try {
    const notifications = await Notification.find({ userId: req.params.userId }).sort({
      createdAt: -1,
    });
    res.json(notifications);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Endpoint: Get notification stats
app.get('/notifications/stats', async (req, res) => {
  try {
    const stats = {
      totalNotifications: await Notification.countDocuments(),
      byType: await Notification.aggregate([
        { $group: { _id: '$type', count: { $sum: 1 } } },
      ]),
      byTopic: await Notification.aggregate([
        { $group: { _id: '$topic', count: { $sum: 1 } } },
      ]),
    };
    res.json(stats);
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
    await consumer.connect();
    console.log('✓ Notification Service: Kafka Consumer connected');

    await startConsumer();

    app.listen(PORT, () => {
      console.log(`Notification Service running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start Notification Service:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down Notification Service...');
  await consumer.disconnect();
  process.exit(0);
});

start();
