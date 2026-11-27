package com.dataextraction.pg.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PostgresLsnTest {

    @Test
    void toStringUsesPostgresHexForm() {
        assertEquals("10/95EBF860", new PostgresLsn("10/95EBF860").toString());
        assertEquals("0/16B3748", new PostgresLsn("0/16B3748").toString());
    }

    @Test
    void fromDebeziumLsnRoundTrips() {
        PostgresLsn lsn = new PostgresLsn("10/84E055B8");
        PostgresLsn again = PostgresLsn.fromDebeziumLsn(lsn.toLong());
        assertEquals(0, lsn.compareTo(again));
        assertTrue(lsn.equals(again));
    }
}
