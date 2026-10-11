package com.dataextraction.pg.meta;

import com.dataextraction.config.SlotProperties;
import java.sql.SQLException;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class PostgresMetaCommands {

    private final PostgresMetaDataDB metadata;
    private final SlotProperties slotProperties;

    public PostgresMetaCommands(PostgresMetaDataDB metadata, SlotProperties slotProperties) {
        this.metadata = metadata;
        this.slotProperties = slotProperties;
    }

    @ShellMethod(key = "postgres-meta", value = "Print the current WAL high water mark and replication slot")
    public String postgresMeta(
            @ShellOption(defaultValue = ShellOption.NULL, help = "Replication slot name") String slot)
            throws SQLException {
        String slotName = slot == null || slot.isBlank() ? slotProperties.getName() : slot;
        PostgresReplicationSlotHighWaterMark highWaterMark = metadata.getHighWaterMark();
        ReplicationSlotInfo slotInfo = metadata.getReplicationSlotStatus(slotName);
        return format(highWaterMark, slotName, slotInfo);
    }

    static String format(
            PostgresReplicationSlotHighWaterMark highWaterMark, String slotName, ReplicationSlotInfo slotInfo) {
        StringBuilder output = new StringBuilder();
        output.append("high water mark: ").append(formatLsn(highWaterMark)).append(System.lineSeparator());
        if (slotInfo == null) {
            output.append("replication slot: not found (").append(slotName).append(")");
            return output.toString();
        }
        output.append("replication slot: ").append(slotInfo.slotName()).append(System.lineSeparator());
        output.append("flush lsn: ").append(slotInfo.flushLSN()).append(System.lineSeparator());
        output.append("restart lsn: ").append(slotInfo.restartLSN()).append(System.lineSeparator());
        output.append("current lsn: ").append(slotInfo.currentLSN()).append(System.lineSeparator());
        output.append("lag bytes: ").append(slotInfo.lagBytes());
        return output.toString();
    }

    private static String formatLsn(PostgresReplicationSlotHighWaterMark highWaterMark) {
        if (highWaterMark == null || highWaterMark.getHighWaterMark() == null) {
            return "unknown";
        }
        PostgresLsn lsn = highWaterMark.getHighWaterMark();
        return "%X/%X".formatted(lsn.getHigh(), lsn.getLow());
    }
}
