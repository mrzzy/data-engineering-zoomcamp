/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - AvroProducer
 */

package kafka_follow_along;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import schemaregistry.RideRecord;

public class AvroProducer {
    private String targetTopic;
    KafkaProducer<String, RideRecord> producer;

    public AvroProducer(Properties kafkaProperties, String targetTopic) {
        // use format specific serializer & deserializer
        kafkaProperties.setProperty(
                "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.setProperty(
                "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        this.targetTopic = targetTopic;
        System.out.println(targetTopic);
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    public void publish(List<Ride> rides) {
        List<RideRecord> records = toRideRecords(rides);
        // send ridges to kafka topic, keyed by pick up loacation (id)
        for (int i = 1; i <= records.size(); i++) {
            RideRecord record = records.get(i - 1);
            try {
                producer.send(
                                new ProducerRecord<String, RideRecord>(
                                        targetTopic, String.valueOf(record.getVendorId()), record))
                        .get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(String.format("Producing Avro: %d/%d", i, records.size()));
        }
    }

    @Override
    protected void finalize() throws Throwable {
        producer.close();
    }

    private List<RideRecord> toRideRecords(List<Ride> rides) {
        return rides.stream()
                .map(
                        ride ->
                                RideRecord.newBuilder()
                                        .setVendorId(ride.vendorID())
                                        .setPassengerCount(ride.passengerCount())
                                        .setTripDistance(ride.tripDistance())
                                        .setPuLocationId(ride.PULocationID())
                                        .build())
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        Properties properties = KafkaProperties.load();
        AvroProducer producer =
                new AvroProducer(
                        properties, properties.getProperty("dezoomcamp.kafka.topic.rides-avro"));
        producer.publish(Records.rides());
    }
}
