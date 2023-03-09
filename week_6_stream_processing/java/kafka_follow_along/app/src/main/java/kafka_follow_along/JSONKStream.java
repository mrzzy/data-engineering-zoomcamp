/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONKStream
 */

package kafka_follow_along;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/** JSONKStream */
public class JSONKStream {
    public static Topology countPickupWindowed(String inRides, String outCounts) {
        // build a topology / dag
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Ride> stream =
                builder.stream(
                        inRides, Consumed.with(Serdes.String(), JSONSerde.build(Ride.class)));

        // create a count cdc stream
        int windowSeconds = 10;
        KStream<Windowed<String>, Long> countStream =
                stream.groupByKey() // group by event key
                        .windowedBy(
                                TimeWindows.ofSizeAndGrace(
                                        Duration.ofSeconds(windowSeconds), Duration.ofSeconds(5)))
                        .count() // count elements in each group into a table
                        .toStream(); // emit changes maded to the count table as stream
        // write count stream to topic
        countStream.to(
                outCounts,
                Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSeconds * 1000),
                        Serdes.Long()));

        return builder.build();
    }

    public static Topology countPickupLocations(String inRides, String outCounts) {
        // build a topology / dag
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Ride> stream =
                builder.stream(
                        inRides, Consumed.with(Serdes.String(), JSONSerde.build(Ride.class)));

        // create a count cdc stream
        KStream<String, Long> countStream =
                stream.groupByKey() // group by event key
                        .count() // count elements in each group into a table
                        .toStream(); // emit changes maded to the count table as stream
        // write count stream to topic
        countStream.to(outCounts, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties properties = KafkaProperties.load();
        Topology topology = null;
        if (args.length > 0 && args[0].equals("window")) {
            topology =
                    countPickupWindowed(
                            properties.getProperty("dezoomcamp.kafka.topic.rides"),
                            properties.getProperty("dezoomcamp.kafka.topic.ride-counts"));
        } else {
            topology =
                    countPickupLocations(
                            properties.getProperty("dezoomcamp.kafka.topic.rides"),
                            properties.getProperty("dezoomcamp.kafka.topic.ride-counts"));
        }
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));
    }
}
