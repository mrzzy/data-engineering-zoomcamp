/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONProducer
 */

package kafka_follow_along;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class JSONProducer<T> {
    private String targetTopic;
    KafkaProducer<String, T> producer;

    public JSONProducer(Properties kafkaProperties, String targetTopic) {
        // use format specific serializer & deserializer
        kafkaProperties.setProperty(
                "value.serializer", "io.confluent.kafka.serializers.KafkaJsonSerializer");
        kafkaProperties.setProperty(
                "value.deserializer", "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        this.targetTopic = targetTopic;
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    public void publish(List<T> records, Function<T, String> makeKey) {
        // send ridges to kafka topic, keyed by pick up loacation (id)
        for (int i = 1; i <= records.size(); i++) {
            T record = records.get(i - 1);
            producer.send(
                    new ProducerRecord<String, T>(targetTopic, makeKey.apply(record), record));
            System.out.println(String.format("Producing: %d/%d", i, records.size()));
        }
    }

    @Override
    protected void finalize() throws Throwable {
        producer.close();
    }

    public static void main(String[] args) {
        Properties properties = KafkaProperties.load();

        if (args[0].equals("rides")) {
            // producer rides
            JSONProducer<Ride> rideProducer =
                    new JSONProducer<>(
                            properties, properties.getProperty("dezoomcamp.kafka.topic.rides"));
            rideProducer.publish(Records.rides(), ride -> String.valueOf(ride.PULocationID()));
        } else if (args[0].equals("zones")) {
            // producer zones
            JSONProducer<Zone> zoneProducer =
                    new JSONProducer<>(
                            properties, properties.getProperty("dezoomcamp.kafka.topic.zones"));
            zoneProducer.publish(Records.zones(), zone -> String.valueOf(zone.locationID()));
        } else if (args[0].equals("locations")) {
            JSONProducer<PickupLocation> puLocationProducer =
                    new JSONProducer<>(
                            properties, properties.getProperty("dezoomcamp.kafka.topic.locations"));
            puLocationProducer.publish(
                    Records.locations(), location -> String.valueOf(location.locationID()));
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + args[0]);
        }
    }
}
