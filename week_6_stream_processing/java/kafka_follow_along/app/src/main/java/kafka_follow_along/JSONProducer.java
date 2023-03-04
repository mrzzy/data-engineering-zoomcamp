/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONProducer
 */

package kafka_follow_along;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class JSONProducer {
    public List<Ride> getRides() throws URISyntaxException {
        URL ridesURL = getClass().getResource("rides.csv");
        // rides url is null if getResource() is unable to find it
        if (ridesURL == null) {
            throw new RuntimeException("Unable to load rides.csv as a resource.");
        }

        // parse rides csv as a list of taxi rides
        try {
            return Files.lines(Path.of(ridesURL.toURI()))
                    // skip csv header
                    .skip(1)
                    .map(line -> Ride.parseTokens(line.split(",")))
                    .collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(
                    String.format("Unexpected exception reading rides.csv: %s\n", e));
        }
    }

    public static void main(String[] args) {}
}
