/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along
 * Zone Model
 */

package kafka_follow_along;

public record Zone(long locationID, String borugh, String zone, String serviceZone) {
    public static Zone parseTokens(String[] tokens) {
        return new Zone(Long.parseLong(tokens[0]), tokens[1], tokens[2], tokens[3]);
    }
}
