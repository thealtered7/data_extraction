package com.dataextraction.kafka;

import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.dataextraction.schemaregistry.SchemaRegistrySerdeProvider;

/**
 * Factory helpers that attach schema-registry-aware value serdes while keeping string keys.
 */
public final class SchemaAwareKafkaProducerFactory {
    private SchemaRegistrySerdeProvider schemaRegistrySerdeProvider;
    private KafkaConfig kafkaConfig;

    public SchemaAwareKafkaProducerFactory(SchemaRegistrySerdeProvider schemaRegistrySerdeProvider, KafkaConfig kafkaConfig) {        
        this.schemaRegistrySerdeProvider = Objects.requireNonNull(schemaRegistrySerdeProvider, "schemaRegistrySerdeProvider");
        this.kafkaConfig = Objects.requireNonNull(kafkaConfig, "kafkaConfig");
    }

    

    public <T> KafkaProducer<String, T> createProducer(
            String bootstrapServers, Class<T> valueType) {
        return createProducer(valueType);
    }

    public  <T> KafkaProducer<String, T> createProducer(Class<T> valueType) {
        Objects.requireNonNull(kafkaConfig, "kafkaConfig");
        Objects.requireNonNull(kafkaConfig.getBootstrapServers(), "kafkaConfig.bootstrapServers");
        Objects.requireNonNull(valueType, "valueType");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        String clientId = kafkaConfig.getClientId();
        if (clientId != null && !clientId.isBlank()) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        return new KafkaProducer<>(
                props, new StringSerializer(), schemaRegistrySerdeProvider.valueSerializer(valueType));
    }

    public <T> KafkaConsumer<String, T> createConsumer(
            Class<T> valueType) {
        Objects.requireNonNull(kafkaConfig, "kafkaConfig");
        Objects.requireNonNull(kafkaConfig.getBootstrapServers(), "kafkaConfig.bootstrapServers");
        Objects.requireNonNull(kafkaConfig.getGroupId(), "kafkaConfig.groupId");
        Objects.requireNonNull(valueType, "valueType");
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getGroupId());
        String clientId = kafkaConfig.getClientId();
        if (clientId != null && !clientId.isBlank()) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(
                props, new StringDeserializer(), schemaRegistrySerdeProvider.valueDeserializer(valueType));
    }
}
