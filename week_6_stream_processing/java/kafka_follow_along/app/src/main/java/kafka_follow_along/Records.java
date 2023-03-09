/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - Records
 */

package kafka_follow_along;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Records {
    private static <R> List<R> get(URL csvURL, Function<String[], R> parseTokens) {
        // rides url is null if getResource() is unable to find it
        if (csvURL == null) {
            throw new RuntimeException(String.format("Unable to load %s as a resource.", csvURL));
        }

        // parse rides csv as a list of taxi rides
        try {
            return Files.lines(Path.of(csvURL.toURI()))
                    // skip csv header
                    .skip(1)
                    // strip quotes & split into fields
                    .map(line -> parseTokens.apply(line.replaceAll("\"", "").split(",")))
                    .collect(Collectors.toList());
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Unexpected exception reading rides.csv:", e);
        }
    }

    public static List<Ride> rides() {
        return Records.get(Records.class.getResource("rides.csv"), Ride::parseTokens);
    }

    public static List<Zone> zones() {
        return Records.get(Records.class.getResource("zones.csv"), Zone::parseTokens);
    }

    public static List<PickupLocation> locations() {
        return List.of(new PickupLocation(186L, LocalDateTime.parse("2020-07-01T00:15:00")));
    }
}
