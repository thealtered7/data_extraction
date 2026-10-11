package com.dataextraction.pg.meta;

import com.dataextraction.config.DatabaseProperties;
import com.dataextraction.observability.Observability;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PostgresMetaConfiguration {

    @Bean
    PostgresConnectionFactory postgresConnectionFactory(DatabaseProperties database) {
        return new PostgresConnectionFactory(
                database.jdbcUrl(), database.getUser(), database.getPassword(), database.getDbname());
    }

    @Bean
    PostgresMetaDataDB postgresMetaDataDB(
            PostgresConnectionFactory connectionFactory, Observability observability) {
        return new PostgresMetaDataDB(connectionFactory, observability);
    }
}
