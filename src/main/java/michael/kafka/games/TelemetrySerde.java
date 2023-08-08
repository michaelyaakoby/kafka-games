package michael.kafka.games;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.HashMap;
import java.util.Map;

public class TelemetrySerde extends SpecificAvroSerde<Telemetry> {

    public TelemetrySerde(SchemaRegistryClient client) {
        super(client);
        configure(Map.of(), false);
    }

    @Override
    public void configure(Map<String, ?> serdeConfig, boolean isSerdeForRecordKeys) {
        Map<String, Object> config = new HashMap<>();
        config.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused");
        config.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        config.putAll(serdeConfig);

        super.configure(config, isSerdeForRecordKeys);
    }
}
