/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along
 * Ride Model
 */

package kafka_follow_along;

import java.time.LocalDateTime;

public record Ride(
        long vendorID,
        LocalDateTime tpepPickupDatetime,
        LocalDateTime tpepDropoffDatetime,
        long passengerCount,
        double tripDistance,
        long RatecodeID,
        String storeAndFwdFlag,
        long PULocationID,
        long DOLocationID,
        long payment_type,
        double fareAmount,
        double extra,
        double mtaTax,
        double tipAmount,
        double tollsAmount,
        double improvementSurcharge,
        double totalAmount,
        double congestionSurcharge) {
    public static Ride parseTokens(String[] tokens) {
        return new Ride(
                Long.parseLong(tokens[0]),
                LocalDateTime.parse(tokens[1].replace(' ', 'T')),
                LocalDateTime.parse(tokens[2].replace(' ', 'T')),
                Long.parseLong(tokens[3]),
                Double.parseDouble(tokens[4]),
                Long.parseLong(tokens[5]),
                tokens[6],
                Long.parseLong(tokens[7]),
                Long.parseLong(tokens[8]),
                Long.parseLong(tokens[9]),
                Double.parseDouble(tokens[10]),
                Double.parseDouble(tokens[11]),
                Double.parseDouble(tokens[12]),
                Double.parseDouble(tokens[13]),
                Double.parseDouble(tokens[14]),
                Double.parseDouble(tokens[15]),
                Double.parseDouble(tokens[16]),
                Double.parseDouble(tokens[17]));
    }
}
