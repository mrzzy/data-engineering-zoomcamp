/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONProducer
 */

package kafka_follow_along;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class JSONProducer<T> {
    private String targetTopic;
    KafkaProducer<String, T> producer;

    public JSONProducer(Properties kafkaProperties, String targetTopic) {
        this.targetTopic = targetTopic;
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    public List<T> getRecords(String resource, Function<String[], T> parseTokens) {
        URL csvURL = getClass().getResource(resource);
        // rides url is null if getResource() is unable to find it
        if (csvURL == null) {
            throw new RuntimeException(String.format("Unable to load %s as a resource.", resource));
        }

        // parse rides csv as a list of taxi rides
        try {
            return Files.lines(Path.of(csvURL.toURI()))
                    // skip csv header
                    .skip(1)
                    // strip quotes & split into fields
                    .map(line -> parseTokens.apply(line.replaceAll("\"", "").split(",")))
                    .collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Unexpected exception reading rides.csv:", e);
        }
    }

    public void publish(List<T> records, Function<T, String> makeKey) {
        // send ridges to kafka topic, keyed by pick up loacation (id)
        for (int i = 1; i <= records.size(); i++) {
            T record = records.get(i - 1);
            producer.send(
                    new ProducerRecord<String, T>(targetTopic, makeKey.apply(record), record));
            System.out.println(String.format("Producing: %d/%d", i, records.size()));
        }
        producer.close();
    }

    public static void main(String[] args) {
        Properties properties = KafkaProperties.load();

        System.out.println(args[0]);
        if (args[0].equals("rides")) {
            // producer rides
            JSONProducer<Ride> rideProducer = new JSONProducer<>(
                    properties, properties.getProperty("dezoomcamp.kafka.topic.rides"));
            rideProducer.publish(
                    rideProducer.getRecords("rides.csv", Ride::parseTokens),
                    ride -> String.valueOf(ride.PULocationID()));
        } else if (args[0].equals("zones")) {
            // producer zones
            JSONProducer<Zone> zoneProducer = new JSONProducer<>(
                    properties, properties.getProperty("dezoomcamp.kafka.topic.zones"));
            zoneProducer.publish(
                    zoneProducer.getRecords("zones.csv", Zone::parseTokens),
                    zone -> String.valueOf(zone.locationID()));
        } else if (args[0].equals("zones")) {
            JSONProducer<PickupLocation> puLocationProducer = new JSONProducer<>(
                    properties, properties.getProperty("dezoomcamp.kafka.topic.locations"));
            puLocationProducer.publish(
                    List.of(new PickupLocation(1212L, LocalDateTime.now())),
                    location -> String.valueOf(location.locationID()));
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + args[0]);
        }
    }
}
