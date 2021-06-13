package com.github.apachekafka.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 To verify the functionality of the Producer.
 * 1. Start zookeeper:
        zookeeper-server-start.sh
            config/zookeeper.properties"

        This will start Zookeeper on 127.0.0.1:2181
 * 2. Start Kafka server:
        kafka-server-start.sh
            config/server.properties"

        This will start Kafka on 127.0.0.1:9092
 * 3. Run
        kafka-topics.sh
            --zookeeper 127.0.0.1:2181
            --list
        to see if we see any results. This verifies connection is good and we are seeing results.
 * 4. Create a TOPIC named "first_topic":
      kafka-topics
         --zookeeper 127.0.0.1:2181
         --topic first_topic
         --create
         --partitions 3
         --replication-factor 1
 * 5. Run this ProducerDemo code, which will produce the message "Hello" to "first_topic".
 * 6. Run a Kafka consumer on this topic "first_topic".
      kafka-console-consumer.sh
            --bootstrap-server 127.0.0.1:9092
            --topic first_topic
            --group my_first_applicatin
    FINAL: We should be able to see the message (produced by the producer) - "Hello" in the Consumer.
 */
public class ProducerDemo {

    public static void main(String[] args) {

        /** Create Producer properties (Refer to https://kafka.apache.org/documentation/#producerconfigs) **/
        Properties properties = new Properties();

        String bootstrapServer = "127.0.0.1:9092"; // Local dddress where Kafka server is running.
        // properties.setProperty("bootstrap.servers", bootstrapServer); // Old way.
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); // BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"

        // Since we went to produce data as String, we need a String serializer. Kafka has it's own list of serializers.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // "value.serializer"

        /** Create Producer **/
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        /** Create Producer Record (Pass in Topic name and data to send) **/
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

        /** Send data **/
        // Since this is Asynchronus, we need to flush and close
        producer.send(record);

        /** Flush data **/
        producer.flush();

        /** Flush and close producer. **/
        producer.close();
    }
}
