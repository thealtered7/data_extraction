package com.dataextraction.kafka;

public class KafkaConfig {
    private String bootstrapServers;
    private String clientId;
    private String groupId;

    public KafkaConfig(String bootstrapServers, String clientId, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.clientId = clientId;
        this.groupId = groupId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getClientId() {
        return clientId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
}
