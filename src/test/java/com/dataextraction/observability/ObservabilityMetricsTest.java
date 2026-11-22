package com.dataextraction.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservabilityMetricsTest {

    @Test
    void recordsCounterAndTimingHistogramWithSourceLabels() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Observability observability = new Observability(ObservationRegistry.create(), registry);
        Map<String, String> tags = Observability.sourceTags("streaming_service_postgres", "geo", "public", "scalars");

        observability.observeOperation(
                Observability.FILE_WRITES_PREFIX,
                "file_store",
                tags,
                () -> "success");

        Counter counter = registry.get(Observability.metricName(Observability.FILE_WRITES_PREFIX, "file_store"))
                .tag(Observability.SOURCE_INSTANCE_NAME, "streaming_service_postgres")
                .tag(Observability.SOURCE_DATABASE_NAME, "geo")
                .tag(Observability.SOURCE_SCHEMA_NAME, "public")
                .tag(Observability.SOURCE_TABLE_NAME, "scalars")
                .tag("outcome", "success")
                .counter();
        assertEquals(1.0, counter.count());

        Timer timer = registry.get(Observability.spanName(Observability.FILE_WRITES_PREFIX, "file_store"))
                .tag(Observability.SOURCE_INSTANCE_NAME, "streaming_service_postgres")
                .tag(Observability.SOURCE_DATABASE_NAME, "geo")
                .tag(Observability.SOURCE_SCHEMA_NAME, "public")
                .tag(Observability.SOURCE_TABLE_NAME, "scalars")
                .tag("outcome", "success")
                .timer();
        assertEquals(1L, timer.count());
    }
}
