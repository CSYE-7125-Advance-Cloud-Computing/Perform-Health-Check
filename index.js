const { Kafka } = require('kafkajs');
const axios = require('axios');
const moment = require('moment-timezone');

require('dotenv').config();

async function performHealthCheck(url) {
    try {
        const response = await axios.get(url, { validateStatus: (status) => true });
        return {
            timestamp: moment.tz('America/New_York').format('YYYY-MM-DD HH:mm:ss'),
            url: url,
            status: response.status,
            expected_status_code: process.env.EXPECTED_STATUS_CODE,
            headers: response.headers
        };
    } catch (error) {
        throw new Error("Health check failed: " + error.message);
    }
}

async function publishToKafka(result, kafkaConfig) {
    const kafka = new Kafka({
        clientId: 'healthCheckProducer',
        brokers: [process.env.KAFKA_SERVER],
        sasl: {
            mechanism: 'plain',
            username: process.env.KAFKA_USER,
            password: process.env.KAFKA_PASSWORD
        },
    });

    const producer = kafka.producer();
    await producer.connect();

    await producer.send({
        topic: kafkaConfig.topic,
        messages: [{ value: JSON.stringify(result) }]
    });

    await producer.disconnect();
}

async function main() {
    // Load Kafka configuration

    const kafkaConfig = {
        bootstrapServers: process.env.KAFKA_SERVER,
        topic: 'healthcheck'
    };

    // Check if SSL is enabled and adjust URI accordingly
    let url = process.env.URI;
    if (process.env.SSL == true) {
        url = `https://${process.env.URI}`;
    } else {
        url = `http://${process.env.URI}`;
    }

    // Retry logic
    const maxRetries = process.env.MAX_RETRIES || 3;
    let retryCount = 0;
    let result = null;

    while (retryCount <= maxRetries) {
        try {
            result = await performHealthCheck(url);
            break; // Exit loop if health check succeeds
        } catch (error) {
            console.error('Health check failed. Retrying...');
            retryCount++;
        }
    }

    if (!result) {
        console.error('Failed to perform health check after maximum retries.');
        process.exit(1);
    }

    // Publish result to Kafka
    try {
        await publishToKafka(result, kafkaConfig);
        console.log('Health check performed and result published to Kafka.');
    } catch (error) {
        console.error('Failed to publish health check result to Kafka:', error.message);
        process.exit(1);
    }

    process.exit(0); 
}

main().catch(error => {
    console.error('An unexpected error occurred:', error);
    process.exit(1);
});