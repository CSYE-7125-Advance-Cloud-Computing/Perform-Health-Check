const { Kafka } = require('kafkajs');

async function performHealthCheck(config) {
    // Implement your health check logic here
    // Example:
    const healthCheckResult = true; // Perform the actual health check and obtain the result
    if (!healthCheckResult) {
        throw new Error("Health check failed");
    }
}

async function publishToKafka(result, kafkaConfig) {
    const kafka = new Kafka({
        clientId: 'healthCheckProducer',
        brokers: [kafkaConfig.bootstrapServers]
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
    // Load health check configuration and Kafka configuration
    // You can use Kubernetes Secrets and ConfigMaps to load these configurations
    const healthCheckConfig = {
        // Your health check configuration here
    };

    const kafkaConfig = {
        bootstrapServers: 'kafka-broker:9092', // Example Kafka broker address
        topic: 'health_check_results' // Example Kafka topic name
    };

    // Perform health check
    const retries = 3;
    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            await performHealthCheck(healthCheckConfig);
            const result = { status: 'success', message: 'Health check passed' };
            await publishToKafka(result, kafkaConfig);
            break;
        } catch (error) {
            const result = { status: 'failed', message: error.message };
            if (attempt < retries - 1) {
                await new Promise(resolve => setTimeout(resolve, 5000)); // Retry after 5 seconds
            } else {
                console.error('Failed to perform health check:', error.message);
                process.exit(1); 
            }
        }
    }
}

main().catch(error => {
    console.error('An unexpected error occurred:', error);
    process.exit(1); 
});
