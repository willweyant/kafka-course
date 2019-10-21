package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        final String bootstrapServer = "127.0.0.1:9092";

        // Create Producer properties
        // Find options here: https://kafka.apache.org/documentation/#producerconfigs
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create producer record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // send data - asynchronous
        producer.send(record);

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
