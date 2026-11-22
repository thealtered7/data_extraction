package com.dataextraction.sink.impl;

import com.dataextraction.observability.Observability;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Span, counter, and timing histogram for one sink operation. Source labels are the tags
 * bound by the owning buffer.
 */
final class SinkTelemetry {

    private Observability observability = Observability.noop();
    private Map<String, String> sourceTags = Map.of();

    void bind(Observability observability, Map<String, String> sourceTags) {
        this.observability = Objects.requireNonNull(observability, "observability");
        this.sourceTags = sourceTags == null ? Map.of() : sourceTags;
    }

    <T> T observe(String operation, Observability.ThrowingSupplier<T> action) throws IOException {
        try {
            return observability.observeCallable(
                    Observability.FILE_WRITES_PREFIX, operation, sourceTags, action);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
