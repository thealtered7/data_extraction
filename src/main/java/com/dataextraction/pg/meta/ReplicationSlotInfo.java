package com.dataextraction.pg.meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public record ReplicationSlotInfo(String databaseName, String slotName, String flushLSN, String restartLSN, String currentLSN, String lagBytes) {
    public static ReplicationSlotInfo fromResultSet(String databaseName, ResultSet resultSet) throws SQLException {
        String slotName = resultSet.getString("slot_name");
        String flushLSN = resultSet.getString("confirmed_flush_lsn");
        String restartLSN = resultSet.getString("restart_lsn");
        String currentLSN = resultSet.getString("current_lsn");
        String lagBytes = resultSet.getString("lag_bytes");
        
        return new ReplicationSlotInfo(
            databaseName,
            slotName,
            flushLSN,
            restartLSN,
            currentLSN,
            lagBytes
        );
    }
}
