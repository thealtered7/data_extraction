package com.dataextraction.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private List<String> brokers = new ArrayList<>();
    private List<String> topics = new ArrayList<>();
    private Map<String, String> advertisedHostOverrides = new LinkedHashMap<>();
    private String clientId;
    private String acks = "all";
    private String groupId;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public List<String> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<String> brokers) {
        this.brokers = brokers;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public Map<String, String> getAdvertisedHostOverrides() {
        return advertisedHostOverrides;
    }

    public void setAdvertisedHostOverrides(Map<String, String> advertisedHostOverrides) {
        this.advertisedHostOverrides = advertisedHostOverrides;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    /** Comma-separated {@code bootstrap.servers} value built from {@link #getBrokers()}. */
    public String getBootstrapServers() {
        if (brokers == null || brokers.isEmpty()) {
            return "";
        }
        return String.join(",", brokers);
    }
}
