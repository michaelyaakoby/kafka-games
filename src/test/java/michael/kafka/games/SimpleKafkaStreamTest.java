package michael.kafka.games;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SimpleKafkaStreamTest extends KafkaTestBase {

    @SneakyThrows
    @Test
    public void testLineSplit() {
        final String INPUT_TOPIC = "line-split-input";
        final String OUTPUT_TOPIC = "line-split-output";

        adminCreateTopics(INPUT_TOPIC, OUTPUT_TOPIC);

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
            .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsProperties("line-split"))) {
            streams.cleanUp();
            streams.start();

            produce(
                INPUT_TOPIC,
                List.of(
                    "all streams lead to kafka",
                    "hello kafka streams",
                    "join kafka summit"
                )
            );

            try (KafkaConsumer<String, String> consumer = newKafkaConsumer("testLineSplit", stringSerde, stringSerde)) {
                consumer.subscribe(List.of(OUTPUT_TOPIC));

                LinkedList<String> producedWords = new LinkedList<>();

                await().untilAsserted(() -> {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    StreamSupport.stream(records.spliterator(), false)
                        .map(ConsumerRecord::value)
                        .forEach(producedWords::add);

                    assertThat(producedWords)
                        .containsExactly(
                            "all", "streams", "lead", "to", "kafka",
                            "hello", "kafka", "streams",
                            "join", "kafka", "summit"
                        );
                });
            }
        }
    }

    @SneakyThrows
    @Test
    public void testWordCount() {
        final String INPUT_TOPIC = "word-count-input";
        final String OUTPUT_TOPIC = "word-count-output";

        adminCreateTopics(INPUT_TOPIC, OUTPUT_TOPIC);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
            // Split each text line, by whitespace, into words.  The text lines are the message
            // values, i.e. we can ignore whatever data is in the message keys and thus invoke
            // `flatMapValues` instead of the more generic `flatMap`.
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            // We use `groupBy` to ensure the words are available as message keys
            .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
            // Count the occurrences of each word (message key).
            .count();

        // Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        wordCounts.toStream().to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        Topology topology = builder.build();
        System.out.print(topology.describe());

        try (final KafkaStreams streams = new KafkaStreams(topology, getStreamsProperties("word-count"))) {
            streams.cleanUp();
            streams.start();

            produce(
                INPUT_TOPIC,
                List.of(
                    "all streams lead to kafka",
                    "hello kafka streams",
                    "join kafka summit"
                )
            );

            try (KafkaConsumer<String, Long> consumer = newKafkaConsumer("testWordCount", stringSerde, longSerde)) {
                consumer.subscribe(List.of(OUTPUT_TOPIC));

                Map<String, Long> producedWordCount = new HashMap<>();

                await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
                    ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(200));
                    StreamSupport.stream(records.spliterator(), false)
                        .forEach(record -> producedWordCount.put(record.key(), record.value()));

                    assertThat(producedWordCount)
                        .isEqualTo(Map.of(
                            "all", 1L,
                            "streams", 2L,
                            "lead", 1L,
                            "kafka", 3L,
                            "to", 1L,
                            "hello", 1L,
                            "join", 1L,
                            "summit", 1L
                        ));
                });
            }
        }

    }

}
