package michael.kafka.games;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class KafkaTestBase {

    static final Serde<String> stringSerde = Serdes.String();
    static final Serde<Long> longSerde = Serdes.Long();

    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @SneakyThrows
    @BeforeAll
    public static void initializeKafkaAndTopics() {
        kafka.start();
    }

    @SneakyThrows
    static void adminCreateTopics(String... topics) {
        try (AdminClient client =
                 AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))
        ) {
            final List<NewTopic> newTopics = Stream.of(topics)
                .map(topicName -> new NewTopic(topicName, 3, (short) 1))
                .toList();
            client.createTopics(newTopics).all().get();
        }
    }

    <K, V> KafkaConsumer<K, V> newKafkaConsumer(String id, Serde<K> keySerde, Serde<V> valueSerde) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, id);
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);

        // If no offset is found, reset to earliest in partition
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(kafkaProperties, keySerde.deserializer(), valueSerde.deserializer());
    }

    <K, V> Consumer<List<V>> newProducer(
        String topic,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer,
        Function<V, K> keyExtractor
    ) {
        return values -> {
            try (KafkaProducer<K, V> producer = newKafkaProducer(keySerializer, valueSerializer)) {
                for (V value : values) {
                    try {
                        producer.send(new ProducerRecord<>(topic, keyExtractor.apply(value), value)).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    @SneakyThrows
    <T> void produce(String topic, List<T> values) {
        try (KafkaProducer<String, T> producer = newKafkaProducer(StringSerializer.class, StringSerializer.class)) {
            for (T value : values) {
                producer.send(new ProducerRecord<>(topic, value)).get();
            }
        }
    }

    <K, V, KS extends Serializer<?>, VS extends Serializer<?>> KafkaProducer<K, V> newKafkaProducer(
        Class<KS> keySerializerClass,
        Class<VS> valueSerializerClass
    ) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        return new KafkaProducer<>(kafkaProperties);
    }

    <K, V> KafkaProducer<K, V> newKafkaProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return new KafkaProducer<>(kafkaProperties, keySerializer, valueSerializer);
    }

    static Properties getStreamsProperties(String appName) {
        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        return streamsProperties;
    }

}
