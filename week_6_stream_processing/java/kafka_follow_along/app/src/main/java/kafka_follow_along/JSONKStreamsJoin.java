/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONKStreamsJoin
 */

package kafka_follow_along;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class JSONKStreamsJoin {
    public Topology build(String inRidesTopic, String inLocationsTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        // read rides & PickupLocation updates from streams
        KStream<String, Ride> rideByDropoff =
                builder.stream(
                        inRidesTopic, Consumed.with(Serdes.String(), JSONSerde.build(Ride.class)));
        KStream<String, PickupLocation> locations =
                builder.stream(
                        inLocationsTopic,
                        Consumed.with(Serdes.String(), JSONSerde.build(PickupLocation.class)));

        // join ride's dropoof & pickup locations together
        KStream<String, Optional<VendorInfo>> vendorOpts =
                rideByDropoff.join(
                        locations,
                        (ValueJoiner<Ride, PickupLocation, Optional<VendorInfo>>)
                                (ride, location) -> {
                                    // only matcch if dropoff & pickup within an event time window
                                    // of 10 minutes
                                    if (Duration.between(
                                                            ride.tpepDropoffDatetime(),
                                                            location.pickupAt())
                                                    .toMinutes()
                                            > 10) {
                                        return Optional.empty();
                                    }
                                    System.out.println("Match!");
                                    return Optional.of(
                                            new VendorInfo(
                                                    ride.vendorID(),
                                                    location.locationID(),
                                                    location.pickupAt(),
                                                    ride.tpepDropoffDatetime()));
                                },
                        // join on processing time window 20min+-5min grace period
                        JoinWindows.ofTimeDifferenceAndGrace(
                                Duration.ofMinutes(20), Duration.ofMinutes(5)),
                        StreamJoined.with(
                                Serdes.String(),
                                JSONSerde.build(Ride.class),
                                JSONSerde.build(PickupLocation.class)));
        // full out empty vendor options
        vendorOpts
                .filter((key, vendorOpt) -> vendorOpt.isPresent())
                // extract vendors
                .mapValues(Optional::get)
                // write vendors to outTopic
                .to(outTopic, Produced.with(Serdes.String(), JSONSerde.build(VendorInfo.class)));
        return builder.build();
    }

    public static void main(String[] args) throws InterruptedException {
        // build kafka streams application
        JSONKStreamsJoin join = new JSONKStreamsJoin();
        Properties properties = KafkaProperties.load();
        KafkaStreams kafkaStreams =
                new KafkaStreams(
                        join.build(
                                properties.getProperty("dezoomcamp.kafka.topic.rides"),
                                properties.getProperty("dezoomcamp.kafka.topic.locations"),
                                properties.getProperty("dezoomcamp.kafka.topic.rides-zones")),
                        properties);
        kafkaStreams.setUncaughtExceptionHandler(
                (e) -> {
                    e.printStackTrace();
                    return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
                });
        kafkaStreams.start();

        // wait for application to start up
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(String.format("Starting: %s", kafkaStreams.state()));
            Thread.sleep(1000);
        }
        System.out.println("Started: Stream Live!");
        while (true) {
            Thread.sleep(30000);
        }
    }
}
