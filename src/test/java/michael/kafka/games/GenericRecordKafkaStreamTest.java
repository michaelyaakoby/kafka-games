package michael.kafka.games;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import michael.kafka.games.avro.DeviceController;
import michael.kafka.games.avro.InverterTelemetry;
import michael.kafka.games.avro.IrradianceSensorTelemetry;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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

public class GenericRecordKafkaStreamTest extends KafkaTestBase {

    private final MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();

    @Test
    void enrichTelemetries() {
        final String INPUT_TOPIC = "telemetry-input";
        final String OUTPUT_TOPIC = "enriched-telemetry-output";
        final String DEVICE_CONTROLLER_TABLE_TOPIC = "controller-site-table";

        adminCreateTopics(INPUT_TOPIC, OUTPUT_TOPIC);

        Serde<DeviceController> deviceControllerSerde = getSpecificAvroSerde(false);
        Serde<Telemetry> telemetrySerde = getSpecificAvroSerde(false);

        final StreamsBuilder builder = new StreamsBuilder();

        ValueJoiner<Telemetry, DeviceController, Telemetry> siteEnricherJoiner = (telemetry, deviceController) -> {
            if (deviceController != null) {
                telemetry.setSiteId(deviceController.getSiteId());
            }
            return telemetry;
        };

        KTable<String, DeviceController> deviceControllerKTable = builder
            .table(DEVICE_CONTROLLER_TABLE_TOPIC, Materialized.with(stringSerde, deviceControllerSerde));

        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, telemetrySerde))
            .selectKey((key, value) -> value.getDeviceControllerSerial().toString())
            .leftJoin(
                deviceControllerKTable,
                siteEnricherJoiner,
                Joined.with(stringSerde, telemetrySerde, deviceControllerSerde)
            )
            .to(OUTPUT_TOPIC, Produced.with(stringSerde, telemetrySerde));

        Topology topology = builder.build();
        System.out.print(topology.describe());

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsProperties("telemetry-enricher"))) {
            streams.cleanUp();
            streams.start();

            newProducer(
                DEVICE_CONTROLLER_TABLE_TOPIC,
                stringSerde.serializer(),
                deviceControllerSerde.serializer(),
                deviceController -> deviceController.getSerial().toString()
            ).accept(List.of(
                new DeviceController("DC74", "PORTIA", "X17"),
                new DeviceController("DC01", "PORTIA", "X17"),
                new DeviceController("DC11", "PORTIA", "A52")
            ));

            Duration duration = Duration.ofSeconds(30);
            long d = duration.toSeconds();
            Instant t0 = Instant.now();
            Instant t1 = t0.plus(duration);
            Instant t2 = t0.plus(duration);
            Instant t3 = t0.plus(duration);
            Instant t4 = t0.plus(duration);

            newProducer(
                INPUT_TOPIC,
                stringSerde.serializer(),
                telemetrySerde.serializer(),
                telemetry -> telemetry.getDeviceControllerSerial().toString()
            ).accept(List.of(
                new InverterTelemetry(t0, "INV10", "DC01", null, d, 123L, 611L, 116L, 721L, 267L),
                new InverterTelemetry(t0, "INV18", "DC74", null, d, 193L, 411L, 136L, 729L, 867L),
                new IrradianceSensorTelemetry(t1, "IRR25", "DC01", null, d, 5772F),
                new InverterTelemetry(t1, "INV72", "DC11", null, d, 822L, 467L, 832L, 128L, 517L),
                new InverterTelemetry(t2, "INV79", "DC11", null, d, 422L, 367L, 802L, 122L, 577L),
                new IrradianceSensorTelemetry(t2, "IRR25", "DC01", null, d, 6267F),

                new InverterTelemetry(t2, "INV10", "DC01", null, d, 123L, 611L, 116L, 721L, 267L),
                new IrradianceSensorTelemetry(t3, "IRR25", "DC01", null, d, 5621F),
                new InverterTelemetry(t3, "INV18", "DC74", null, d, 197L, 811L, 133L, 429L, 863L),
                new InverterTelemetry(t3, "INV72", "DC11", null, d, 852L, 417L, 872L, 138L, 557L),
                new InverterTelemetry(t4, "INV79", "DC11", null, d, 522L, 363L, 402L, 127L, 177L)
            ));


            try (KafkaConsumer<String, Telemetry> consumer = newKafkaConsumer("telemetry-enricher-test", stringSerde, telemetrySerde)) {
                consumer.subscribe(List.of(OUTPUT_TOPIC));

                LinkedList<Telemetry> producedEnrichedTelemetries = new LinkedList<>();

                await().untilAsserted(() -> {
                    ConsumerRecords<String, Telemetry> records = consumer.poll(Duration.ofMillis(200));
                    StreamSupport.stream(records.spliterator(), false)
                        .map(ConsumerRecord::value)
                        .forEach(producedEnrichedTelemetries::add);

                    assertThat(producedEnrichedTelemetries)
                        .containsExactlyInAnyOrder(
                            new InverterTelemetry(t0, "INV10", "DC01", "X17", d, 123L, 611L, 116L, 721L, 267L),
                            new InverterTelemetry(t0, "INV18", "DC74", "X17", d, 193L, 411L, 136L, 729L, 867L),
                            new InverterTelemetry(t1, "INV72", "DC11", "A52", d, 822L, 467L, 832L, 128L, 517L),
                            new InverterTelemetry(t2, "INV79", "DC11", "A52", d, 422L, 367L, 802L, 122L, 577L),

                            new InverterTelemetry(t2, "INV10", "DC01", "X17", d, 123L, 611L, 116L, 721L, 267L),
                            new InverterTelemetry(t3, "INV18", "DC74", "X17", d, 197L, 811L, 133L, 429L, 863L),
                            new InverterTelemetry(t3, "INV72", "DC11", "A52", d, 852L, 417L, 872L, 138L, 557L),
                            new InverterTelemetry(t4, "INV79", "DC11", "A52", d, 522L, 363L, 402L, 127L, 177L),

                            new IrradianceSensorTelemetry(t1, "IRR25", "DC01", "X17", d, 5772F),
                            new IrradianceSensorTelemetry(t2, "IRR25", "DC01", "X17", d, 6267F),
                            new IrradianceSensorTelemetry(t3, "IRR25", "DC01", "X17", d, 5621F)
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
