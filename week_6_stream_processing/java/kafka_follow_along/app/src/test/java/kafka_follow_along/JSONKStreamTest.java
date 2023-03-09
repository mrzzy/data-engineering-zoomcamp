/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONKStreamTest
 */

package kafka_follow_along;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** JSONKStreamTest */
public class JSONKStreamTest {
    private TopologyTestDriver driver;
    private TestInputTopic<String, Ride> inTopic;
    private TestOutputTopic<String, Long> outTopic;

    @BeforeEach
    public void setup() {
        Properties properties = KafkaProperties.load();
        String inRides = properties.getProperty("dezoomcamp.kafka.topic.rides");
        String outCounts = properties.getProperty("dezoomcamp.kafka.topic.ride-counts");
        driver =
                new TopologyTestDriver(
                        JSONKStream.countPickupLocations(inRides, outCounts), properties);
        inTopic =
                driver.createInputTopic(
                        inRides,
                        Serdes.String().serializer(),
                        JSONSerde.build(Ride.class).serializer());
        outTopic =
                driver.createOutputTopic(
                        outCounts, Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @Test
    public void testIfMessageCountsEqualToMessageSent() throws Exception {
        // produce rides into the test topic
        List<Ride> rides = Records.rides();
        rides.forEach((ride) -> inTopic.pipeInput(String.valueOf(ride.PULocationID()), ride));
        assertEquals(rides.size(), outTopic.getQueueSize());

        Map<String, Long> expected =
                rides.stream()
                        .collect(Collectors.groupingBy(Ride::PULocationID))
                        .entrySet()
                        .stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> String.valueOf(entry.getKey()),
                                        entry -> (long) entry.getValue().size()));
        assertEquals(expected, outTopic.readKeyValuesToMap());
    }

    @AfterEach
    public void teardown() {
        driver.close();
    }
}
