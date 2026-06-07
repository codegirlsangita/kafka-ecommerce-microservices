const { Kafka } = require('kafkajs');

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const TOPICS = [
  process.env.KAFKA_TOPIC_USER_CREATED || 'user.created',
  process.env.KAFKA_TOPIC_ORDER_CREATED || 'order.created',
  process.env.KAFKA_TOPIC_PAYMENT_COMPLETED || 'payment.completed',
];
const TOPIC_PARTITIONS = Number(process.env.KAFKA_TOPIC_PARTITIONS || 3);
const TOPIC_REPLICATION_FACTOR = Number(process.env.KAFKA_TOPIC_REPLICATION_FACTOR || 1);

const kafka = new Kafka({
  clientId: 'topic-creator',
  brokers: KAFKA_BROKERS,
});

const admin = kafka.admin();

async function createTopics() {
  try {
    await admin.connect();
    console.log('[OK] Connected to Kafka admin');

    const existingTopics = await admin.listTopics();
    console.log('Existing topics:', existingTopics, '\n');

    for (const topicName of TOPICS) {
      if (existingTopics.includes(topicName)) {
        console.log(`[SKIP] Topic '${topicName}' already exists`);
      } else {
        console.log(`Creating topic '${topicName}'...`);
        await admin.createTopics({
          topics: [{
            topic: topicName,
            numPartitions: TOPIC_PARTITIONS,
            replicationFactor: TOPIC_REPLICATION_FACTOR,
          }],
          validateOnly: false,
          timeout: 10000,
        });
        console.log(`[OK] Topic '${topicName}' created successfully`);
      }
    }

    const allTopics = await admin.listTopics();
    console.log('\n[OK] All topics:', allTopics);
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  } finally {
    await admin.disconnect();
  }
}

createTopics();
