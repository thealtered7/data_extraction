package com.dataextraction.notify.impl;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.notify.events.FileWriteFlushEvent;

public class FileWriteObserverKafka {
    private static final Logger logger = LoggerFactory.getLogger(FileWriteObserverKafka.class);

    private Producer<String, Object> producer;
    private String[] publishTopics;

    public FileWriteObserverKafka(Producer<String, Object> producer, String[] publishTopics) {
        this.producer = producer;
        this.publishTopics = publishTopics;
    }

    private String getEventId(FileWriteFlushEvent event) {
        String instanceName = event.instanceName();
        if (instanceName == null) {
            instanceName = "unknown";
        }   
        String databaseName = event.databaseName();
        if (databaseName == null) {
            databaseName = "unknown";
        }
        String schemaName = event.schemaName();
        if (schemaName == null) {
            schemaName = "unknown";
        }
        String tableName = event.tableName();

        return String.format("%s.%s.%s.%s", instanceName, databaseName, schemaName, tableName);
    }
    
    public void observe(FileWriteFlushEvent event) {
        logger.info("Observing file write flush event: {}", event);
        String key = this.getEventId(event);
        for (String publishTopic : this.publishTopics) {
            producer.send(new ProducerRecord<String, Object>(publishTopic, key, event));
        }
    }
}
