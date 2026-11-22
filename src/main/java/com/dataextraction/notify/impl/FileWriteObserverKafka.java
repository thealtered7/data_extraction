package com.dataextraction.notify.impl;

import java.util.Objects;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.notify.events.FileWriteFlushEvent;
import com.dataextraction.observability.Observability;

public class FileWriteObserverKafka implements FileWrittenObserver {
    private static final Logger logger = LoggerFactory.getLogger(FileWriteObserverKafka.class);

    private final Producer<String, FileWriteFlushEvent> producer;
    private final String[] publishTopics;
    private final Observability observability;

    private FileWriteObserverKafka(Builder builder) {
        Objects.requireNonNull(builder.producer, "producer is required");
        Objects.requireNonNull(builder.publishTopics, "publishTopics is required");
        Objects.requireNonNull(builder.observability, "observability is required");
        this.producer = builder.producer;
        this.publishTopics = builder.publishTopics;
        this.observability = builder.observability;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Producer<String, FileWriteFlushEvent> producer;
        private String[] publishTopics;
        private Observability observability;

        public Builder withProducer(Producer<String, FileWriteFlushEvent> producer) {
            this.producer = producer;
            return this;
        }

        public Builder withPublishTopics(String[] publishTopics) {
            this.publishTopics = publishTopics;
            return this;
        }

        public FileWriteObserverKafka build() {
            return new FileWriteObserverKafka(this);
        }

        public Builder withObservability(Observability observability) {
            this.observability = observability;
            return this;
        }
    }

    private String getEventId(FileWriteFlushEvent event) {
        String instanceName = event.getSourceInstanceName();
        if (instanceName == null) {
            instanceName = "unknown";
        }
        String databaseName = event.getSourceDatabaseName();
        if (databaseName == null) {
            databaseName = "unknown";
        }
        String schemaName = event.getSourceSchemaName();
        if (schemaName == null) {
            schemaName = "unknown";
        }
        String tableName = event.getSourceTableName();

        return String.format("%s.%s.%s.%s", instanceName, databaseName, schemaName, tableName);
    }
    
    public void observe(FileWriteFlushEvent event) {
        logger.info("Observing file write flush event: {}", event);

        try {
            observability.observeCallableVoid(
                Observability.FILE_WRITES_PREFIX,
                "kafka_publish",
                Observability.sourceTags(
                        event.getSourceInstanceName(),
                        event.getSourceDatabaseName(),
                        event.getSourceSchemaName(),
                        event.getSourceTableName()),
                () -> {
                    this.do_observe(event);
                    return "success";
                });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish file write flush event", e);
        }
    }

    private void do_observe(FileWriteFlushEvent event) {
        String key = this.getEventId(event);
        for (String publishTopic : this.publishTopics) {
            producer.send(new ProducerRecord<>(publishTopic, key, event));
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }
}
