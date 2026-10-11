package com.dataextraction.pg.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnectionFactory {
    private final String connectionUrl;
    private final String user;
    private final String password;
    private final String instanceName;

    public PostgresConnectionFactory(String connectionUrl, String instanceName) {
        this(connectionUrl, null, null, instanceName);
    }

    public PostgresConnectionFactory(String connectionUrl, String user, String password, String instanceName) {
        this.connectionUrl = connectionUrl;
        this.user = user;
        this.password = password;
        this.instanceName = instanceName;
    }

    public Connection getConnection() throws SQLException {
        if (user == null) {
            return DriverManager.getConnection(connectionUrl);
        }
        return DriverManager.getConnection(connectionUrl, user, password);
    }

    public String getInstanceName() {
        return instanceName;
    }
}
