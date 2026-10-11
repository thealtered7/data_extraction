package com.dataextraction.hello;

import com.dataextraction.config.DataExtractionProperties;
import com.dataextraction.hello.impl.FartHelloer;
import com.dataextraction.hello.impl.SimpleHelloer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HelloConfiguration {

    @Bean
    Helloer helloer(DataExtractionProperties properties) {
        return switch (properties.getHelloer()) {
            case SIMPLE -> new SimpleHelloer();
            case FART -> new FartHelloer();
        };
    }
}
