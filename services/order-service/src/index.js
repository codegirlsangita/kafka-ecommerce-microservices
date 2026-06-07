const express = require('express');
const mongoose = require('mongoose');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const app = express();
app.use(express.json());

const SERVICE_NAME = 'order-service';
const PORT = Number(process.env.ORDER_SERVICE_PORT || 3002);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const MONGODB_URI = process.env.ORDER_MONGODB_URI
  || process.env.MONGODB_URI
  || 'mongodb://admin:password@localhost:27017/order_db?authSource=admin';
const USER_CREATED_TOPIC = process.env.KAFKA_TOPIC_USER_CREATED || 'user.created';
const ORDER_CREATED_TOPIC = process.env.KAFKA_TOPIC_ORDER_CREATED || 'order.created';
const CONSUMER_GROUP_ID = process.env.KAFKA_CONSUMER_GROUP_ORDER || 'order-service-group';

// Initialize Kafka
// This service is both a PRODUCER and CONSUMER
// Producer: publishes "order.created" events
// Consumer: listens to "user.created" events
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || SERVICE_NAME,
  brokers: KAFKA_BROKERS,
});

const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
});

// Consumer: listens to user.created topic
// Consumer Group: 'order-service-group'
// Offset: 'earliest' = read from beginning if group is new
const consumer = kafka.consumer({
  groupId: CONSUMER_GROUP_ID,
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
});

// MongoDB Connection
mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => console.log('Order Service: MongoDB connected'))
  .catch(err => console.error('MongoDB connection error:', err));

// Order Schema
const orderSchema = new mongoose.Schema({
  userId: { type: String, required: true },
  items: [
    {
      productId: String,
      quantity: Number,
      price: Number,
    },
  ],
  totalAmount: { type: Number, required: true },
  status: {
    type: String,
    enum: ['pending', 'confirmed', 'shipped', 'delivered'],
    default: 'pending',
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

const Order = mongoose.model('Order', orderSchema);

// Endpoint: Create a new order
// This is a PRODUCER - publishes "order.created" event
app.post('/orders', async (req, res) => {
  try {
    const { userId, items, totalAmount } = req.body;

    if (!userId || !items || !totalAmount) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const order = new Order({ userId, items, totalAmount });
    await order.save();

    // Publish order.created event
    await producer.send({
      topic: ORDER_CREATED_TOPIC,
      messages: [
        {
          key: userId, // Partition by userId - all orders from same user go to same partition
          value: JSON.stringify({
            orderId: order._id,
            userId: order.userId,
            totalAmount: order.totalAmount,
            itemCount: items.length,
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });

    console.log(`[ORDER SERVICE] Published order.created event for order: ${order._id}`);

    res.status(201).json({
      message: 'Order created successfully',
      order: {
        id: order._id,
        userId,
        totalAmount,
        status: order.status,
      },
    });
  } catch (error) {
    console.error('Error creating order:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint: Get all orders
app.get('/orders', async (req, res) => {
  try {
    const orders = await Order.find();
    res.json(orders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Endpoint: Get order by ID
app.get('/orders/:id', async (req, res) => {
  try {
    const order = await Order.findById(req.params.id);
    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }
    res.json(order);
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

// CONSUMER: Listen to "user.created" topic
// This demonstrates how one service can react to events from another service
async function startConsumer() {
  try {
    await consumer.subscribe({
      topic: USER_CREATED_TOPIC,
      // fromBeginning: true, // Uncomment to read from beginning (useful for testing)
    });

    // Run consumer
    // eachMessage callback is called for each message received
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          console.log(
            `[ORDER SERVICE] Received user.created event:`,
            event,
            `\n  Partition: ${partition}, Offset: ${message.offset}`
          );

          // Store user info or prepare for orders
          // In real scenario, you might log this to trigger welcome campaigns, etc.
        } catch (error) {
          console.error('Error processing message:', error.message);
        }
      },
    });

    console.log('✓ Order Service: Consumer listening to user.created topic');
  } catch (error) {
    console.error('Error starting consumer:', error);
  }
}

// Initialize Kafka and start server
async function start() {
  try {
    await producer.connect();
    console.log('✓ Order Service: Kafka Producer connected');

    await consumer.connect();
    console.log('✓ Order Service: Kafka Consumer connected');

    await startConsumer();

    app.listen(PORT, () => {
      console.log(`Order Service running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start Order Service:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down Order Service...');
  await producer.disconnect();
  await consumer.disconnect();
  process.exit(0);
});

start();
