package com.dataextraction;

import com.dataextraction.config.DataExtractionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataExtractionProperties.class)
public class DataExtractionApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataExtractionApplication.class, args);
    }
}
