/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONConsumer
 */

package kafka_follow_along;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public class JSONKStreamsJoin {
    public Topology build(String inRidesTopic, String inLocationsTopic, String outTopic) {
    }

    public static void main(String[] args) throws InterruptedException {
        // build kafka streams application
        JSONKStreamsJoin join = new JSONKStreamsJoin();
        Properties properties = KafkaProperties.load();
        KafkaStreams kafkaStreams = new KafkaStreams(join.build(
                properties.getProperty("dezoomcamp.kafka.topic.rides"),
                properties.getProperty("dezoomcamp.kafka.topic.location"),
                properties.getProperty("dezoomcamp.kafka.topic.out")),
                properties);
        kafkaStreams.start();

        // wait for application to start up
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(String.format("Starting: %s", kafkaStreams.state()));
            Thread.sleep(1000);
        }
        System.out.println("Started: Stream Live!");
    }
}
