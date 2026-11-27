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

    /**
     * How far the active walsender has read. This advances through WAL that contains no
     * publication changes, which leaves {@code confirmed_flush_lsn} unchanged.
     */
    /**
     * True when the slot's walsender has read every flushed WAL record and is waiting for new
     * WAL. {@code sent_lsn} is null for roles without {@code pg_read_all_stats}; this wait event
     * is visible to the replication user.
     */
    public boolean isWalSenderWaitingForWal(String slotName) throws SQLException {
        Map<String, String> tags = new HashMap<>();
        tags.put("instance_name", this.getInstanceName());
        tags.put("slot_name", slotName);
        try {
            return observability.observeCallable(
                    Observability.POSTGRES_METADATA_PREFIX,
                    "is_wal_sender_waiting_for_wal",
                    tags,
                    () -> queryWalSenderWaitingForWal(slotName));
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Error reading walsender wait for slot " + slotName, e);
        }
    }

    private boolean queryWalSenderWaitingForWal(String slotName) throws SQLException {
        Connection connection = connectionFactory.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                    SELECT pg_stat_activity.wait_event = 'WalSenderWaitForWAL' AS waiting_for_wal
                    FROM pg_replication_slots
                    JOIN pg_stat_activity
                      ON pg_stat_activity.pid = pg_replication_slots.active_pid
                    WHERE pg_replication_slots.slot_name = ?
                    """);
            preparedStatement.setString(1, slotName);
            ResultSet resultSet = preparedStatement.executeQuery();
            try {
                if (!resultSet.next()) {
                    return false;
                }
                return resultSet.getBoolean("waiting_for_wal");
            } finally {
                resultSet.close();
            }
        } finally {
            connection.close();
        }
    }

    public PostgresLsn getWalSenderSentLsn(String slotName) throws SQLException {
        Map<String, String> tags = new HashMap<>();
        tags.put("instance_name", this.getInstanceName());
        tags.put("slot_name", slotName);
        try {
            return observability.observeCallable(
                    Observability.POSTGRES_METADATA_PREFIX,
                    "get_wal_sender_sent_lsn",
                    tags,
                    () -> queryWalSenderSentLsn(slotName));
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Error getting walsender sent_lsn for slot " + slotName, e);
        }
    }

    private PostgresLsn queryWalSenderSentLsn(String slotName) throws SQLException {
        Connection connection = connectionFactory.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                    SELECT pg_stat_replication.sent_lsn
                    FROM pg_replication_slots
                    JOIN pg_stat_replication
                      ON pg_stat_replication.pid = pg_replication_slots.active_pid
                    WHERE pg_replication_slots.slot_name = ?
                    """);
            preparedStatement.setString(1, slotName);
            ResultSet resultSet = preparedStatement.executeQuery();
            try {
                if (!resultSet.next()) {
                    return null;
                }
                String sentLsn = resultSet.getString("sent_lsn");
                if (sentLsn == null || sentLsn.isBlank()) {
                    return null;
                }
                return new PostgresLsn(sentLsn);
            } finally {
                resultSet.close();
            }
        } finally {
            connection.close();
        }
    }

    public String getInstanceName() {
        if (this.connectionFactory != null) {
            return this.connectionFactory.getInstanceName();
        }
        return "UNKNOWN";
    }

    public void logReplicationSlotStatus(String slotName) throws SQLException {
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
                    return;
                }
                log.info(
                        "Replication slot {}: confirmed_flush_lsn={}, restart_lsn={}, current_lsn={}, lag_bytes={}",
                        resultSet.getString("slot_name"),
                        resultSet.getString("confirmed_flush_lsn"),
                        resultSet.getString("restart_lsn"),
                        resultSet.getString("current_lsn"),
                        resultSet.getString("lag_bytes"));
            } finally {
                resultSet.close();
            }
        } finally {
            connection.close();
        }
    }
}
