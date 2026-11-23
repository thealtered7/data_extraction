package com.dataextraction.cdc_consumer.format;

import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.buffer.AvroExtractBufferFactory;
import com.dataextraction.buffer.JsonExtractBufferFactory;
import com.dataextraction.cdc_consumer.AvroRecordDelegateFactory;
import com.dataextraction.cdc_consumer.JsonRecordDelegateFactory;
import com.dataextraction.cdc_consumer.SnapShotFileWriteConsumer;
import com.dataextraction.cdc_consumer.impl.BufferedFileWriteConsumer;
import com.dataextraction.config.DebeziumProperties;
import com.dataextraction.config.SchemaRegistryProperties;
import com.dataextraction.schemaregistry.SchemaRegistryBackend;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Json;

/**
 * Builds the engine, deserializer, record delegate, and buffer for {@code debezium.format}.
 * Snapshot and streaming share one selection.
 */
public abstract class ChangeFormatSupport {

    private static final Logger log = LoggerFactory.getLogger(ChangeFormatSupport.class);

    public static ChangeFormatSupport from(
            DebeziumProperties debeziumProperties, SchemaRegistryProperties schemaRegistryProperties) {
        ChangeFormat format = debeziumProperties == null ? ChangeFormat.JSON : debeziumProperties.getFormat();
        log.info("Debezium change format: {}", format.name().toLowerCase());
        if (format == ChangeFormat.AVRO) {
            if (schemaRegistryProperties == null
                    || schemaRegistryProperties.getSchemaRegistryBackend() != SchemaRegistryBackend.CONFLUENT
                    || schemaRegistryProperties.getConfluent() == null
                    || schemaRegistryProperties.getConfluent().getUrl() == null
                    || schemaRegistryProperties.getConfluent().getUrl().isBlank()) {
                throw new IllegalStateException("debezium.format=avro requires schema-registry.confluent.url");
            }
            return new AvroSupport(
                    schemaRegistryProperties.getConfluent().getUrl(),
                    schemaRegistryProperties.getConfluent().isAutoRegisterSchemas());
        }
        return new JsonSupport();
    }

    public final StartedSnapshot startSnapshot(
            Properties connectorProps,
            SnapshotDependencies dependencies,
            DebeziumEngine.CompletionCallback callback) throws IOException {
        SnapShotFileWriteConsumer<?> consumer = snapshotConsumer(dependencies);
        consumer.init();
        DebeziumEngine<?> engine = openEngine(connectorProps, consumer, callback);
        return new StartedSnapshot(engine, engine, consumer);
    }

    public final BufferedFileWriteConsumer<?> streamingConsumer(StreamingDependencies dependencies) {
        return newStreamingConsumer(dependencies);
    }

    public final DebeziumEngine<?> openEngine(
            Properties connectorProps,
            Consumer<?> consumer,
            DebeziumEngine.CompletionCallback callback) {
        configureEngine(connectorProps);
        return newEngine(connectorProps, consumer, callback);
    }

    protected abstract void configureEngine(Properties connectorProps);

    protected abstract SnapShotFileWriteConsumer<?> snapshotConsumer(SnapshotDependencies dependencies);

    protected abstract BufferedFileWriteConsumer<?> newStreamingConsumer(StreamingDependencies dependencies);

    protected abstract DebeziumEngine<?> newEngine(
            Properties connectorProps,
            Consumer<?> consumer,
            DebeziumEngine.CompletionCallback callback);

    private static final class JsonSupport extends ChangeFormatSupport {

        @Override
        protected void configureEngine(Properties connectorProps) {
        }

        @Override
        protected SnapShotFileWriteConsumer<?> snapshotConsumer(SnapshotDependencies dependencies) {
            return dependencies.build(
                    JsonRecordDeserializer.create(),
                    JsonRecordDelegateFactory.create(),
                    new JsonExtractBufferFactory());
        }

        @Override
        protected BufferedFileWriteConsumer<?> newStreamingConsumer(StreamingDependencies dependencies) {
            return dependencies.build(
                    JsonRecordDeserializer.create(),
                    JsonRecordDelegateFactory.create(),
                    new JsonExtractBufferFactory());
        }

        @Override
        protected DebeziumEngine<?> newEngine(
                Properties connectorProps,
                Consumer<?> consumer,
                DebeziumEngine.CompletionCallback callback) {
            return DebeziumEngine.create(Json.class)
                    .using(connectorProps)
                    .using(callback)
                    .notifying(cast(consumer))
                    .build();
        }

        @SuppressWarnings("unchecked")
        private static Consumer<ChangeEvent<String, String>> cast(Consumer<?> consumer) {
            return (Consumer<ChangeEvent<String, String>>) consumer;
        }
    }

    private static final class AvroSupport extends ChangeFormatSupport {

        private final String schemaRegistryUrl;
        private final boolean autoRegisterSchemas;

        private AvroSupport(String schemaRegistryUrl, boolean autoRegisterSchemas) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.autoRegisterSchemas = autoRegisterSchemas;
        }

        @Override
        protected void configureEngine(Properties connectorProps) {
            String autoRegister = Boolean.toString(autoRegisterSchemas);
            connectorProps.setProperty("converter.schema.registry.url", schemaRegistryUrl);
            connectorProps.setProperty("key.converter.schema.registry.url", schemaRegistryUrl);
            connectorProps.setProperty("value.converter.schema.registry.url", schemaRegistryUrl);
            connectorProps.setProperty("converter.auto.register.schemas", autoRegister);
            connectorProps.setProperty("key.converter.auto.register.schemas", autoRegister);
            connectorProps.setProperty("value.converter.auto.register.schemas", autoRegister);
        }

        @Override
        protected SnapShotFileWriteConsumer<?> snapshotConsumer(SnapshotDependencies dependencies) {
            return dependencies.build(
                    AvroRecordDeserializer.create(schemaRegistryUrl),
                    AvroRecordDelegateFactory.create(),
                    new AvroExtractBufferFactory());
        }

        @Override
        protected BufferedFileWriteConsumer<?> newStreamingConsumer(StreamingDependencies dependencies) {
            return dependencies.build(
                    AvroRecordDeserializer.create(schemaRegistryUrl),
                    AvroRecordDelegateFactory.create(),
                    new AvroExtractBufferFactory());
        }

        @Override
        protected DebeziumEngine<?> newEngine(
                Properties connectorProps,
                Consumer<?> consumer,
                DebeziumEngine.CompletionCallback callback) {
            return DebeziumEngine.create(Avro.class)
                    .using(connectorProps)
                    .using(callback)
                    .notifying(cast(consumer))
                    .build();
        }

        @SuppressWarnings("unchecked")
        private static Consumer<ChangeEvent<byte[], byte[]>> cast(Consumer<?> consumer) {
            return (Consumer<ChangeEvent<byte[], byte[]>>) consumer;
        }
    }
}
