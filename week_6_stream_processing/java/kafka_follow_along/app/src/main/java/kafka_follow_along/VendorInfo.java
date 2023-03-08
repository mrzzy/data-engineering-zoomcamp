/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - VendorInfo
 */

package kafka_follow_along;

import java.time.LocalDateTime;

public record VendorInfo(
        long vendorID,
        long PULocationID,
        LocalDateTime pickupTime,
        LocalDateTime lastDropoffTime) {}
