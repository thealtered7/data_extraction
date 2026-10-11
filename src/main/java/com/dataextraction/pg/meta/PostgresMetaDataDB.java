package com.dataextraction.pg.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.dataextraction.observability.Observability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresMetaDataDB {
    private static final Logger log = LoggerFactory.getLogger(PostgresMetaDataDB.class);

    private PostgresConnectionFactory connectionFactory;
    private final Observability observability;

    public PostgresMetaDataDB(PostgresConnectionFactory connectionFactory) {
        this(connectionFactory, Observability.noop());
    }

    public PostgresMetaDataDB(
            PostgresConnectionFactory connectionFactory, Observability observability) {
        this.connectionFactory = connectionFactory;
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    public PostgresReplicationSlotHighWaterMark getHighWaterMark() throws SQLException {
        Map<String, String> tags = new HashMap<>();
        tags.put("instance_name", this.getInstanceName());        
        try {
            return observability.observeCallable(
                    Observability.POSTGRES_METADATA_PREFIX,
                    "get_high_water_mark",
                    tags,
                    this::queryHighWaterMark);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Error getting replication slot high water mark", e);
        }
    }

    private PostgresReplicationSlotHighWaterMark queryHighWaterMark() throws SQLException {
        Connection connection = connectionFactory.getConnection();
        try {
            PreparedStatement preparedStatement =
                    connection.prepareStatement("SELECT pg_current_wal_lsn() AS highWaterMark");
            ResultSet resultSet = preparedStatement.executeQuery();
            try {
                resultSet.next();
                String highWaterMark = resultSet.getString("highWaterMark");
                PostgresLsn postgresLsn = new PostgresLsn(highWaterMark);
                PostgresReplicationSlotHighWaterMark postgresReplicationSlotHighWaterMark =
                        new PostgresReplicationSlotHighWaterMark();
                postgresReplicationSlotHighWaterMark.setHighWaterMark(postgresLsn);
                return postgresReplicationSlotHighWaterMark;
            } finally {
                resultSet.close();
            }
        } finally {
            connection.close();
        }
    }

    private ReplicationSlotInfo queryReplicationSlotStatus(String slotName) throws SQLException {
        Connection connection = connectionFactory.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                    SELECT slot_name,
                           confirmed_flush_lsn,
                           restart_lsn,
                           pg_current_wal_lsn() AS current_lsn,
                           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
                    FROM pg_replication_slots
                    WHERE slot_name = ?
                    """);
            preparedStatement.setString(1, slotName);
            ResultSet resultSet = preparedStatement.executeQuery();
            try {
                if (!resultSet.next()) {
                    log.warn("Replication slot not found: {}", slotName);
                    return null;
                }
                return ReplicationSlotInfo.fromResultSet(this.getInstanceName(), resultSet);
            } finally {
                resultSet.close();
            }
        } finally {
            connection.close();
        }
    }

    public ReplicationSlotInfo getReplicationSlotStatus(String slotName) throws SQLException {
        Map<String, String> tags = new HashMap<>();
        tags.put("instance_name", this.getInstanceName());
        tags.put("slot_name", slotName);
        try {
            return observability.observeCallable(
                    Observability.POSTGRES_METADATA_PREFIX,
                    "get_replication_slot_status",
                    tags,
                    () -> queryReplicationSlotStatus(slotName));
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Error getting replication slot status", e);
        }
    }

    public String getInstanceName() {
        if (this.connectionFactory != null) {
            return this.connectionFactory.getInstanceName();
        }
        return "UNKNOWN";
    }
}
