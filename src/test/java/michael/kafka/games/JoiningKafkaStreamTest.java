package michael.kafka.games;

import michael.kafka.games.avro.ApplianceOrder;
import michael.kafka.games.avro.CombinedOrder;
import michael.kafka.games.avro.ElectronicOrder;
import michael.kafka.games.avro.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class JoiningKafkaStreamTest extends KafkaTestBase {

    private final MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();

    @Test
    void joinStreamsAndTable() {
        final String APPLIANCES_ORDER_INPUT_TOPIC = "appliance-order";
        final String ELECTRONICS_ORDER_TOPIC = "electronic-order";
        final String USERS_TABLE_TOPIC = "user-table";
        final String OUTPUT_TOPIC = "combined-order";

        adminCreateTopics(APPLIANCES_ORDER_INPUT_TOPIC, ELECTRONICS_ORDER_TOPIC, OUTPUT_TOPIC);

        final StreamsBuilder builder = new StreamsBuilder();

        Serde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(false);
        Serde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(false);
        Serde<CombinedOrder> combinedSerde = getSpecificAvroSerde(false);
        Serde<User> userSerde = getSpecificAvroSerde(false);

        ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
            (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                .setApplianceOrderId(applianceOrder.getOrderId())
                .setApplianceId(applianceOrder.getApplianceId())
                .setElectronicOrderId(electronicOrder.getOrderId())
                .setUserName(applianceOrder.getUserId())
                .setTime(Instant.now().toEpochMilli())
                .build();

        ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {
                combined.setUserName(user.getName());
            }
            return combined;
        };

        KStream<String, ApplianceOrder> applianceStream = builder
            .stream(APPLIANCES_ORDER_INPUT_TOPIC, Consumed.with(stringSerde, applianceSerde))
            .peek((key, value) -> System.out.println("Appliance-order stream incoming record key " + key + " value " + value));

        KStream<String, ElectronicOrder> electronicStream = builder
            .stream(ELECTRONICS_ORDER_TOPIC, Consumed.with(stringSerde, electronicSerde))
            .peek((key, value) -> System.out.println("Electronic-order stream incoming record " + key + " value " + value));

        KTable<String, User> userTable = builder
            .table(USERS_TABLE_TOPIC, Materialized.with(stringSerde, userSerde));

        KStream<String, CombinedOrder> combinedStream = applianceStream
            .join(
                electronicStream,
                orderJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(stringSerde, applianceSerde, electronicSerde)
            )
            .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));

        combinedStream
            .selectKey((key, value) -> value.getUserName().toString())
            .leftJoin(
                userTable,
                enrichmentJoiner,
                Joined.with(stringSerde, combinedSerde, userSerde)
            )
            .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
            .to(OUTPUT_TOPIC, Produced.with(stringSerde, combinedSerde));

        Topology topology = builder.build();
        System.out.print(topology.describe());

        try (final KafkaStreams streams = new KafkaStreams(topology, getStreamsProperties("join-streams-and-table"))) {
            streams.cleanUp();
            streams.start();

            newProducer(
                USERS_TABLE_TOPIC,
                stringSerde.serializer(),
                userSerde.serializer(),
                user -> user.getUserId().toString()
            ).accept(List.of(
                new User("Elizabeth Jones", "5405 6th Avenue", "10261998"),
                new User("Art Vandelay", "407 64th Street", "10261999")
            ));

            newProducer(
                APPLIANCES_ORDER_INPUT_TOPIC,
                stringSerde.serializer(),
                applianceSerde.serializer(),
                applianceOrder -> applianceOrder.getOrderId().toString()
            ).accept(List.of(
                new ApplianceOrder("remodel-1", "dishwasher-1333", "10261998", Instant.now().toEpochMilli()),
                new ApplianceOrder("remodel-2", "stove-2333", "10261999", Instant.now().toEpochMilli())
            ));

            newProducer(
                ELECTRONICS_ORDER_TOPIC,
                stringSerde.serializer(),
                electronicSerde.serializer(),
                electronicOrder -> electronicOrder.getOrderId().toString()
            ).accept(List.of(
                new ElectronicOrder("remodel-1", "television-2333", "10261998", 0d, Instant.now().toEpochMilli()),
                new ElectronicOrder("remodel-2", "laptop-5333", "10261999", 0d, Instant.now().toEpochMilli())
            ));

            try (KafkaConsumer<String, CombinedOrder> consumer = newKafkaConsumer("joinStreamsAndTable", stringSerde, combinedSerde)) {
                consumer.subscribe(List.of(OUTPUT_TOPIC));

                LinkedList<CombinedOrder> producedCombinedOrders = new LinkedList<>();

                await().untilAsserted(() -> {
                    ConsumerRecords<String, CombinedOrder> records = consumer.poll(Duration.ofMillis(200));
                    StreamSupport.stream(records.spliterator(), false)
                        .map(ConsumerRecord::value)
                        .peek(c -> c.setTime(0L)) // Zeroing timestamp to enable comparison
                        .forEach(producedCombinedOrders::add);

                    assertThat(producedCombinedOrders)
                        .containsExactly(
                            new CombinedOrder("remodel-1", "remodel-1", "dishwasher-1333", "Elizabeth Jones", 0L),
                            new CombinedOrder("remodel-2", "remodel-2", "stove-2333", "Art Vandelay", 0L)
                        );
                });
            }

        }
    }

    public <T extends SpecificRecord> Serde<T> getSpecificAvroSerde(boolean isKey) {
        return Serdes.serdeFrom(getSerializer(isKey), getDeserializer(isKey));
    }

    private <T> Serializer<T> getSerializer(boolean isKey) {
        Map<String, Object> map = new HashMap<>();
        map.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
        map.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused");
        Serializer<T> serializer = (Serializer) new KafkaAvroSerializer(mockSchemaRegistryClient);
        serializer.configure(map, isKey);
        return serializer;
    }

    private <T> Deserializer<T> getDeserializer(boolean key) {
        Map<String, Object> map = new HashMap<>();
        map.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        map.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused");
        Deserializer<T> deserializer = (Deserializer) new KafkaAvroDeserializer(mockSchemaRegistryClient);
        deserializer.configure(map, key);
        return deserializer;
    }
}
