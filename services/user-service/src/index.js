const express = require('express');
const mongoose = require('mongoose');
const { Kafka, CompressionTypes } = require('kafkajs');
require('dotenv').config();

const app = express();
app.use(express.json());

const SERVICE_NAME = 'user-service';
const PORT = Number(process.env.USER_SERVICE_PORT || 3001);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const MONGODB_URI = process.env.USER_MONGODB_URI
  || process.env.MONGODB_URI
  || 'mongodb://admin:password@localhost:27017/user_db?authSource=admin';
const USER_CREATED_TOPIC = process.env.KAFKA_TOPIC_USER_CREATED || 'user.created';

// Initialize Kafka Producer
// Kafka Producer: sends messages to topics
// This service publishes "user.created" events when new users are registered
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || SERVICE_NAME,
  brokers: KAFKA_BROKERS,
});

const producer = kafka.producer({
  // idempotent: true enables exactly-once semantics
  idempotent: true,
  maxInFlightRequests: 5,
  compression: CompressionTypes.GZIP,
});

// MongoDB Connection
mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => console.log('User Service: MongoDB connected'))
  .catch(err => console.error('MongoDB connection error:', err));

// User Schema
const userSchema = new mongoose.Schema({
  email: { type: String, required: true, unique: true },
  name: { type: String, required: true },
  password: { type: String, required: true },
  createdAt: { type: Date, default: Date.now },
});

const User = mongoose.model('User', userSchema);

// Endpoint: Register a new user
// This is a PRODUCER - sends an event to Kafka
app.post('/users', async (req, res) => {
  try {
    const { email, name, password } = req.body;

    // Validate input
    if (!email || !name || !password) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Save user to MongoDB
    const user = new User({ email, name, password });
    await user.save();

    // Publish event to Kafka topic "user.created"
    // Key: used for partitioning (same user goes to same partition)
    // Value: event data in JSON
    await producer.send({
      topic: USER_CREATED_TOPIC,
      messages: [
        {
          key: user._id.toString(),
          value: JSON.stringify({
            userId: user._id,
            email: user.email,
            name: user.name,
            timestamp: new Date().toISOString(),
          }),
          // partition: 0, // Optional: specify partition (for learning purposes)
        },
      ],
    });

    console.log(`[USER SERVICE] Published user.created event for user: ${user._id}`);

    res.status(201).json({
      message: 'User created successfully',
      user: {
        id: user._id,
        email: user.email,
        name: user.name,
      },
    });
  } catch (error) {
    console.error('Error creating user:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint: Get all users
app.get('/users', async (req, res) => {
  try {
    const users = await User.find().select('-password');
    res.json(users);
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

// Endpoint: Get user by ID
app.get('/users/:id', async (req, res) => {
  try {
    const user = await User.findById(req.params.id).select('-password');
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.json(user);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Initialize Kafka producer and start server
async function start() {
  try {
    await producer.connect();
    console.log('✓ User Service: Kafka Producer connected');

    app.listen(PORT, () => {
      console.log(`User Service running on port ${PORT}`);
    });
  } catch (error) {
    console.error('Failed to start User Service:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down User Service...');
  await producer.disconnect();
  process.exit(0);
});

start();
