/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - KafkaProperties Loader
 */

package kafka_follow_along;

import java.io.IOException;
import java.util.Properties;

public class KafkaProperties {
    public static Properties load() {
        // load default properties from kafka properties file
        Properties properties = new Properties();
        try {
            properties.load(
                    KafkaProperties.class
                            .getClassLoader()
                            .getResourceAsStream("kafka_follow_along/kafka.properties"));
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
