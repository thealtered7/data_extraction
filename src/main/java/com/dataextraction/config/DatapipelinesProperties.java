package com.dataextraction.config;

import java.net.http.HttpClient;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.dataextraction.datapipelines.DatapipelinesHttpClient;
import com.dataextraction.datapipelines.JwtTokenProvider;
import com.dataextraction.observability.Observability;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@ConfigurationProperties(prefix = "datapipelines")
public class DatapipelinesProperties {

    private String baseUrl;    
    private JwtProperties jwt;
    private int requestTimeoutSeconds = 10;

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setJwt(JwtProperties jwt) {
        this.jwt = jwt;
    }

    public JwtProperties getJwt() {
        return jwt;
    }

    public void setRequestTimeoutSeconds(int requestTimeoutSeconds) {
        this.requestTimeoutSeconds = requestTimeoutSeconds;
    }

    public int getRequestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    /**
     * Returns a client when {@code app.datapipelines.http.base-url} is set; otherwise null.
     */
    public DatapipelinesHttpClient createDatePipelinesHttpClient(Observability observability) {
        Duration requestTimeout = Duration.ofSeconds(getRequestTimeoutSeconds());
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(requestTimeout)
                .build();
        JwtTokenProvider jwtTokenProvider = getJwt().buildJwtTokenProvider();
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        DatapipelinesHttpClient client = new DatapipelinesHttpClient(
                httpClient, mapper, observability, getBaseUrl(), jwtTokenProvider);
        return client;
    }    
}
