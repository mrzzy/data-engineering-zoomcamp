/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONKStreamsJoinTest
 */

package kafka_follow_along;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JSONKStreamsJoinTest {
    private TopologyTestDriver driver;
    private TestInputTopic<String, Ride> inRides;
    private TestInputTopic<String, PickupLocation> inLocations;
    private TestOutputTopic<String, VendorInfo> outTopic;

    @BeforeEach
    public void setup() {
        Properties properties = KafkaProperties.load();
        String inRidesStr = properties.getProperty("dezoomcamp.kafka.topic.rides");
        String inLocationsStr = properties.getProperty("dezoomcamp.kafka.topic.locations");
        String outTopicStr = properties.getProperty("dezoomcamp.kafka.topic.rides-zones");

        driver =
                new TopologyTestDriver(
                        JSONKStreamsJoin.build(inRidesStr, inLocationsStr, outTopicStr),
                        properties);
        inRides =
                driver.createInputTopic(
                        inRidesStr,
                        Serdes.String().serializer(),
                        JSONSerde.build(Ride.class).serializer());
        inLocations =
                driver.createInputTopic(
                        inLocationsStr,
                        Serdes.String().serializer(),
                        JSONSerde.build(PickupLocation.class).serializer());
        outTopic =
                driver.createOutputTopic(
                        outTopicStr,
                        Serdes.String().deserializer(),
                        JSONSerde.build(VendorInfo.class).deserializer());
    }

    @Test
    public void testIfJoinWorksOneSameDropoffPickupLocationId() throws Exception {
        Ride ride =
                Ride.parseTokens(
                        ("1,2020-07-01 00:07:57,2020-07-01 00:14:42,"
                                        + "2,1.70,1,N,186,142,2,7.5,3,0.5,0,0,0.3,11.3,2.5")
                                .split(","));
        inRides.pipeInput(String.valueOf(ride.PULocationID()), ride);
        PickupLocation location = Records.locations().get(0);
        inLocations.pipeInput(String.valueOf(location.locationID()), location);

        assertEquals(1, outTopic.getQueueSize());
        var message = outTopic.readKeyValue();
        assertEquals(String.valueOf(location.locationID()), message.key);
        assertEquals(
                new VendorInfo(
                        ride.vendorID(),
                        location.locationID(),
                        location.pickupAt(),
                        ride.tpepDropoffDatetime()),
                message.value);
    }

    @AfterEach
    public void teardown() {
        driver.close();
    }
}
