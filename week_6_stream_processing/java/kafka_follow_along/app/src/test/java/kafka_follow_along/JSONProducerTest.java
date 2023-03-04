/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONProducer Unit Tests
 */

package kafka_follow_along;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

public class JSONProducerTest {
    private JSONProducer jsonProducer = new JSONProducer();

    @Test
    public void testGetRides() throws Exception {
        var rides = jsonProducer.getRides();
        assertEquals(rides.size(), 266);
        assertEquals(rides.get(0).vendorID(), 1);
        assertEquals(rides.get(0).tpepPickupDatetime(), LocalDateTime.parse("2020-07-01T00:25:32"));
        assertEquals(
                rides.get(0).tpepDropoffDatetime(), LocalDateTime.parse("2020-07-01T00:38:57"));
        assertEquals(rides.get(0).passengerCount(), 1);
        assertEquals(rides.get(0).tripDistance(), 14.2, 1e-15);
        assertEquals(rides.get(0).RatecodeID(), 1);
        assertEquals(rides.get(0).storeAndFwdFlag(), "N");
        assertEquals(rides.get(0).PULocationID(), 75);
        assertEquals(rides.get(0).DOLocationID(), 236);
        assertEquals(rides.get(0).payment_type(), 1);
        assertEquals(rides.get(0).fareAmount(), 7.0, 1e-15);
        assertEquals(rides.get(0).extra(), 0.5, 1e-15);
        assertEquals(rides.get(0).mtaTax(), 0.5, 1e-15);
        assertEquals(rides.get(0).tipAmount(), 2.7, 1e-15);
        assertEquals(rides.get(0).tollsAmount(), 0.0, 1e-15);
        assertEquals(rides.get(0).improvementSurcharge(), 0.3, 1e-15);
        assertEquals(rides.get(0).totalAmount(), 13.5, 1e-15);
        assertEquals(rides.get(0).congestionSurcharge(), 2.5, 1e-15);
    }
}
