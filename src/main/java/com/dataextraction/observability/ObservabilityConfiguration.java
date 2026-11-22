package com.dataextraction.observability;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ObservabilityConfiguration {

    @Bean(destroyMethod = "close")
    ObservabilityFactory observabilityFactory() {
        return ObservabilityFactory.getInstance();
    }

    @Bean
    Observability observability(ObservabilityFactory observabilityFactory) {
        return observabilityFactory.observability();
    }
}
