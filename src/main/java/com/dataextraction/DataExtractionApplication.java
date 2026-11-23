package com.dataextraction;

import com.dataextraction.config.CatalogProperties;
import com.dataextraction.config.DataExtractionProperties;
import com.dataextraction.config.DatabaseProperties;
import com.dataextraction.config.DebeziumProperties;
import com.dataextraction.config.DatapipelinesProperties;
import com.dataextraction.config.FileWriterProperties;
import com.dataextraction.config.KafkaProperties;
import com.dataextraction.config.SchemaRegistryProperties;
import com.dataextraction.config.SinkProperties;
import com.dataextraction.config.SlotProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
    CatalogProperties.class,
    DataExtractionProperties.class,
    DatabaseProperties.class,
    DebeziumProperties.class,
    DatapipelinesProperties.class,
    FileWriterProperties.class,
    KafkaProperties.class,
    SchemaRegistryProperties.class,
    SinkProperties.class,
    SlotProperties.class
})
public class DataExtractionApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataExtractionApplication.class, args);
    }
}
