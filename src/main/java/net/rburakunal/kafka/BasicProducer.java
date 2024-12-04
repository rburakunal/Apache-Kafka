package net.rburakunal.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BasicProducer {

    // Kafka topic name
    public static final String TOPIC = "hello-world-topic";

    public static void main(String[] args) {
        System.out.println("*** Starting Basic Producer ***");

        // Kafka Producer configuration settings
        Properties settings = new Properties();
        settings.put("client.id", "basic-producer");
        settings.put("bootstrap.servers", "localhost:9092");
        settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Try-with-resources to ensure the producer is closed automatically
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(settings)) {
            // Loop to send 5 messages
            for (int i = 1; i <= 5; i++) {
                final String key = "key-" + i;   // Key for the message
                final String value = "value-" + i; // Value for the message

                System.out.println("### Sending message " + i + " ###");
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                // Send the record
                producer.send(record);

                // Print confirmation
                System.out.println("Message sent: key=" + key + ", value=" + value);
            }
        } catch (Exception e) {
            // Handle any exceptions
            e.printStackTrace();
        }

        System.out.println("*** Basic Producer Finished ***");
    }
}
