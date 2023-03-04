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
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class JSONProducer {
    public List<Ride> getRides() {
        URL ridesURL = getClass().getResource("rides.csv");
        // rides url is null if getResource() is unable to find it
        if (ridesURL == null) {
            throw new RuntimeException("Unable to load rides.csv as a resource.");
        }

        // parse rides csv as a list of taxi rides
        try {
            return Files.lines(Path.of(ridesURL.toURI()))
                    // skip csv header
                    .skip(1)
                    .map(line -> Ride.parseTokens(line.split(",")))
                    .collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Unexpected exception reading rides.csv:", e);
        }
    }

    public void publishRides(List<Ride> rides) {
        Properties properties = loadProperties();
        KafkaProducer<String, Ride> producer = new KafkaProducer<>(properties);
        // send ridges to kafka topic, keyed by pick up loacation (id)
        for (int i = 1; i <= rides.size(); i++) {
            Ride ride = rides.get(i - 1);
            producer.send(
                    new ProducerRecord<String, Ride>(
                            properties.getProperty("kafka.producer.topic"),
                            String.valueOf(ride.PULocationID()),
                            ride));
            System.out.println(String.format("Producing: %d/%d", i, rides.size()));
        }
        producer.close();
    }

    public static void main(String[] args) {
        JSONProducer producer = new JSONProducer();
        producer.publishRides(producer.getRides());
    }

    private Properties loadProperties() {
        // load default properties from kafka properties file
        Properties properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load defaults from kafka.properties: ", e);
        }
        // apply credentials from process environment into Properties
        properties.setProperty(
                "sasl.jaas.config",
                String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule"
                                + " required username='%s' password='%s';",
                        getEnv("CLUSTER_API_KEY"), getEnv("CLUSTER_API_SECRET")));
        return properties;
    }

    private static String getEnv(String key) {
        String value = System.getenv(key);
        if (value == null) {
            throw new RuntimeException(String.format("Expected environment variable: %s", key));
        }
        return value;
    }
}
