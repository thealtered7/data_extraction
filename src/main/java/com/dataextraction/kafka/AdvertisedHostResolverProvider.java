package com.dataextraction.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolver.LookupPolicy;
import java.net.spi.InetAddressResolverProvider;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites broker hostnames from Kafka metadata before the client connects.
 * The JVM loads this provider through {@code ServiceLoader}; {@link #apply} installs the map.
 */
public final class AdvertisedHostResolverProvider extends InetAddressResolverProvider {
    private static final Logger log = LoggerFactory.getLogger(AdvertisedHostResolverProvider.class);

    private static volatile AdvertisedHostResolver installed;

    public static void apply(Map<String, String> overrides) {
        Map<String, String> filtered = filter(overrides);
        ensureInstalled();
        AdvertisedHostResolver resolver = installed;
        if (resolver == null) {
            log.warn("Kafka advertised-host resolver is not installed");
            return;
        }
        resolver.setOverrides(filtered);
        if (!filtered.isEmpty()) {
            log.info("Kafka advertised host overrides: {}", filtered);
        }
    }

    private static Map<String, String> filter(Map<String, String> overrides) {
        Map<String, String> filtered = new LinkedHashMap<>();
        if (overrides == null) {
            return Map.copyOf(filtered);
        }
        for (Map.Entry<String, String> entry : overrides.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                continue;
            }
            if (entry.getValue() == null || entry.getValue().isBlank()) {
                continue;
            }
            filtered.put(entry.getKey(), entry.getValue());
        }
        return Map.copyOf(filtered);
    }

    private static void ensureInstalled() {
        if (installed != null) {
            return;
        }
        try {
            InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            log.debug("localhost lookup while installing advertised-host resolver", e);
        }
    }

    @Override
    public InetAddressResolver get(Configuration configuration) {
        AdvertisedHostResolver resolver = new AdvertisedHostResolver(configuration.builtinResolver());
        installed = resolver;
        return resolver;
    }

    @Override
    public String name() {
        return "dataextraction-advertised-host";
    }

    static final class AdvertisedHostResolver implements InetAddressResolver {
        private static final Logger log = LoggerFactory.getLogger(AdvertisedHostResolver.class);

        private final InetAddressResolver delegate;
        private volatile Map<String, String> overrides = Map.of();

        AdvertisedHostResolver(InetAddressResolver delegate) {
            this.delegate = delegate;
        }

        void setOverrides(Map<String, String> overrides) {
            this.overrides = overrides;
        }

        @Override
        public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy)
                throws UnknownHostException {
            return delegate.lookupByName(mappedHost(host), lookupPolicy);
        }

        @Override
        public String lookupByAddress(byte[] addr) throws UnknownHostException {
            return delegate.lookupByAddress(addr);
        }

        private String mappedHost(String host) {
            for (Map.Entry<String, String> entry : overrides.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(host)) {
                    log.debug("resolving advertised host {} as {}", host, entry.getValue());
                    return entry.getValue();
                }
            }
            return host;
        }
    }
}
