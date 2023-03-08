/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONProducer Unit Tests
 */

package kafka_follow_along;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class JSONProducerTest {
    private final Properties properties = KafkaProperties.load();

    @Test
    public void testGetRideRecords() throws Exception {
        final JSONProducer<Ride> ridesProducer = new JSONProducer<>(properties, "test");
        List<Ride> rides = ridesProducer.getRecords("rides.csv", Ride::parseTokens);
        assertEquals(rides.size(), 266);
        assertEquals(rides.get(0).vendorID(), 1);
        assertEquals(rides.get(0).tpepPickupDatetime(), LocalDateTime.parse("2020-07-01T00:25:32"));
        assertEquals(
                rides.get(0).tpepDropoffDatetime(), LocalDateTime.parse("2020-07-01T00:33:39"));
        assertEquals(rides.get(0).passengerCount(), 1);
        assertEquals(rides.get(0).tripDistance(), 1.5, 1e-15);
        assertEquals(rides.get(0).RatecodeID(), 1);
        assertEquals(rides.get(0).storeAndFwdFlag(), "N");
        assertEquals(rides.get(0).PULocationID(), 238);
        assertEquals(rides.get(0).DOLocationID(), 75);
        assertEquals(rides.get(0).payment_type(), 2);
        assertEquals(rides.get(0).fareAmount(), 8.0, 1e-15);
        assertEquals(rides.get(0).extra(), 0.5, 1e-15);
        assertEquals(rides.get(0).mtaTax(), 0.5, 1e-15);
        assertEquals(rides.get(0).tipAmount(), 0.0, 1e-15);
        assertEquals(rides.get(0).tollsAmount(), 0.0, 1e-15);
        assertEquals(rides.get(0).improvementSurcharge(), 0.3, 1e-15);
        assertEquals(rides.get(0).totalAmount(), 9.3, 1e-15);
        assertEquals(rides.get(0).congestionSurcharge(), 0.0, 1e-15);
    }

    @Test
    public void testGetZoneRecords() throws Exception {
        final JSONProducer<Zone> zonesProducer = new JSONProducer<>(properties, "test");
        List<Zone> zones = zonesProducer.getRecords("zones.csv", Zone::parseTokens);
        assertEquals(zones.size(), 265);
        assertEquals(zones.get(0).locationID(), 1);
        assertEquals(zones.get(0).borugh(), "EWR");
        assertEquals(zones.get(0).zone(), "Newark Airport");
        assertEquals(zones.get(0).serviceZone(), "EWR");
    }
}
