package michael.kafka.games;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import lombok.Getter;
import michael.kafka.games.avro.DeviceController;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;

@Getter
public class TelemetrySiteEnricher {

    static final Serde<String> stringSerde = Serdes.String();

    private final Topology topology;

    public TelemetrySiteEnricher(
        SchemaRegistryClient schemaRegistryClient,
        String telemetriesInputTopic,
        String telemetriesOutputTopic,
        String deviceControllerTableTopic
    ) {
        Serde<DeviceController> deviceControllerSerde = getSpecificAvroSerde(schemaRegistryClient);
        Serde<Telemetry> telemetrySerde = getSpecificAvroSerde(schemaRegistryClient);

        final StreamsBuilder builder = new StreamsBuilder();

        ValueJoiner<Telemetry, DeviceController, Telemetry> siteEnricherJoiner = (telemetry, deviceController) -> {
            if (deviceController != null) {
                telemetry.setSiteId(deviceController.getSiteId());
            }
            return telemetry;
        };

        KTable<String, DeviceController> deviceControllerKTable = builder
            .table(deviceControllerTableTopic, Materialized.with(stringSerde, deviceControllerSerde));

        builder.stream(telemetriesInputTopic, Consumed.with(stringSerde, telemetrySerde))
            .selectKey((key, value) -> value.getDeviceControllerSerial().toString())
            .leftJoin(
                deviceControllerKTable,
                siteEnricherJoiner,
                Joined.with(stringSerde, telemetrySerde, deviceControllerSerde)
            )
            .to(telemetriesOutputTopic, Produced.with(stringSerde, telemetrySerde));

        topology = builder.build();
    }

    public static <T extends SpecificRecord> Serde<T> getSpecificAvroSerde(SchemaRegistryClient schemaRegistryClient) {
        return (Serde<T>) new TelemetrySerde(schemaRegistryClient);
    }

}
