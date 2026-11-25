const { Kafka } = require('kafkajs');
const ssl = process.env.KAFKA_SSL === 'true';
module.exports = {
  TOPIC: 'tenant.orders',
  kafka: new Kafka({
    clientId: 'saas-platform',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    ssl,
    sasl: ssl ? { mechanism: process.env.KAFKA_MECHANISM || 'scram-sha-256',
                  username: process.env.KAFKA_USER, password: process.env.KAFKA_PASS } : undefined,
  }),
};
