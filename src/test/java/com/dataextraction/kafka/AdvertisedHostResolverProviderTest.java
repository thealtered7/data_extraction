package com.dataextraction.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AdvertisedHostResolverProviderTest {

    @AfterEach
    void clearOverrides() {
        AdvertisedHostResolverProvider.apply(Map.of());
    }

    @Test
    void rewritesAdvertisedHostToConfiguredAddress() throws UnknownHostException {
        AdvertisedHostResolverProvider.apply(Map.of("kafka-advertised.test", "127.0.0.1"));

        InetAddress[] addresses = InetAddress.getAllByName("kafka-advertised.test");

        assertEquals(1, addresses.length);
        assertEquals("127.0.0.1", addresses[0].getHostAddress());
    }

    @Test
    void blankOverrideLeavesHostUnresolved() {
        AdvertisedHostResolverProvider.apply(Map.of("kafka-blank.test", " "));

        assertThrows(UnknownHostException.class, () -> InetAddress.getAllByName("kafka-blank.test"));
    }
}
