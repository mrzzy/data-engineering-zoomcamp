/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONConsumer
 */

package kafka_follow_along;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class JSONConsumer {
    private final KafkaConsumer<String, Ride> consumer;

    JSONConsumer() {
        Properties properties = KafkaProperties.load();
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(properties.getProperty("dezoomcamp.kafka.topic")));
    }

    public void consumeRides() {
        ConsumerRecords<String, Ride> records = null;
        do {
            records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, Ride> record : records) {
                System.out.println(String.format("consuming: %s", record.value().DOLocationID()));
            }
        } while (records.isEmpty());
    }

    public static void main(String[] args) {
        JSONConsumer consumer = new JSONConsumer();
        consumer.consumeRides();
    }
}
