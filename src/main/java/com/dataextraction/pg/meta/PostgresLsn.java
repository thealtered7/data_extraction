package com.dataextraction.pg.meta;

import java.util.Objects;

public class PostgresLsn {
    private Long high;
    private Long low;

    public PostgresLsn(long high, long low) {
        Objects.requireNonNull(high, "high must not be null");
        Objects.requireNonNull(low, "low must not be null");
        this.high = high;
        this.low = low;
    }

    public PostgresLsn(String lsn) {
        String[] parts = lsn.split("\\/");
        this.high = Long.parseLong(parts[0], 16);
        this.low = Long.parseLong(parts[1], 16);
    }

    public PostgresLsn(String high, String low) {
        this.high = Long.parseLong(high, 16);
        this.low = Long.parseLong(low, 16);
    }   

    public Long getHigh() {
        return high;
    }

    public Long getLow() {
        return low;
    }

    public Long toLong() {
        return (high << 32) | low;
    }

    public static PostgresLsn fromDebeziumLsn(long debeziumLsn) {
        long high = debeziumLsn >>> 32;
        long low = debeziumLsn & 0xFFFFFFFFL;
        return new PostgresLsn(high, low);
    }

    public int compareTo(PostgresLsn other) {
        int val = this.getHigh().compareTo(other.getHigh());
        if (val == 0) {
            val = this.getLow().compareTo(other.getLow());
        }
        return val;
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return this.compareTo((PostgresLsn) other) == 0;
    }

    public String toString() {
        return this.high + "/" + this.low;
    }
}
