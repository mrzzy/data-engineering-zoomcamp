/*
 * data-engineering-zoomcamp
 * Week 6
 * Kafka Follow Along - JSONSerde:w
 *
*/
package kafka_follow_along;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

public class JSONSerde {
    // Pass class as parameter 'cls' as Java type erasure removes type info when compiling.
    public static <T> Serde<T> build(Class<T> cls) {
        Serde<T> serde = Serdes.serdeFrom(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>());
        // configure deserialiser to encode/decode to Rides class instead of generic
        // HashMap
        serde.configure(Map.of("json.value.type", cls.getName()), false);
        return serde;
    }

}
