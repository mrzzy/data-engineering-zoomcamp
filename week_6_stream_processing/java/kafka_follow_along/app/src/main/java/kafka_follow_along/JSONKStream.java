/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONKStream
 */

package kafka_follow_along;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/** JSONKStream */
public class JSONKStream {
    public static Topology countPickupLocations(Properties properties) {
        // build a topology / dag
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Ride> stream =
                builder.stream(
                        properties.getProperty("dezoomcamp.kafka.topic.rides"),
                        Consumed.with(Serdes.String(), JSONSerde.build(Ride.class)));

        // create a count cdc stream
        KStream<String, Long> countStream =
                stream.groupByKey() // group by event key
                        .count() // count elements in each group into a table
                        .toStream(); // emit changes maded to the count table as stream
        // write count stream to topic
        countStream.to(
                properties.getProperty("dezoomcamp.kafka.topic.ride-counts"),
                Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties properties = KafkaProperties.load();
        KafkaStreams kafkaStreams =
                new KafkaStreams(JSONKStream.countPickupLocations(properties), properties);
        kafkaStreams.start();
    }
}
