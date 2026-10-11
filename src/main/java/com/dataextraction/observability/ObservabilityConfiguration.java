package com.dataextraction.observability;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ObservabilityConfiguration {

    @Bean(destroyMethod = "close")
    ObservabilityFactory observabilityFactory() {
        return ObservabilityFactory.create();
    }

    @Bean
    Observability observability(ObservabilityFactory observabilityFactory) {
        return observabilityFactory.observability();
    }
}
