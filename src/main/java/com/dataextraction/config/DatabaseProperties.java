package com.dataextraction.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "database")
public class DatabaseProperties {

    private String hostname = "localhost";
    private int port = 5432;
    private String user = "postgres";
    private String password = "";
    private String dbname = "postgres";

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
}
