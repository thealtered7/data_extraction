package com.dataextraction.pg.meta;

public class PostgresReplicationSlotHighWaterMark {
    private PostgresLsn highWaterMark; 

    public PostgresLsn getHighWaterMark() {
        return highWaterMark;
    }

    public void setHighWaterMark(PostgresLsn highWaterMark) {
        this.highWaterMark = highWaterMark;
    }
}
