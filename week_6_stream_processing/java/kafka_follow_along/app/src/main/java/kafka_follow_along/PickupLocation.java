/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - PickupLocation
 */

package kafka_follow_along;

import java.time.LocalDateTime;

public record PickupLocation(long locationID, LocalDateTime atTimestamp) {}
