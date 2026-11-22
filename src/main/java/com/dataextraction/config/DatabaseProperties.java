package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "source-database")
public class DatabaseProperties {

    private String hostname = "localhost";
    private int port = 5432;
    private String user = "postgres";
    private String password = "";
    private String dbname = "postgres";
    private String slotName = "extract_slot";
    private String publicationName = "extract_publication";
    private String offsetStorageFileFilename = "offset.json";
    private String instanceName = "geo-database";

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/%s".formatted(hostname, port, dbname);
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public void setPublicationName(String publicationName) {
        this.publicationName = publicationName;
    }

    public String getOffsetStorageFileFilename() {
        return offsetStorageFileFilename;
    }

    public void setOffsetStorageFileFilename(String offsetStorageFileFilename) {
        this.offsetStorageFileFilename = offsetStorageFileFilename;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
}
