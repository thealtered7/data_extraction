package com.dataextraction.observability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

public class Observability {

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    public static final String SERVICE_NAME = "extract_data";

    public static final String POSTGRES_METADATA_PREFIX = SERVICE_NAME + ".postgres_metadata";
    public static final String FILE_WRITES_PREFIX = SERVICE_NAME + ".file_writes";
    public static final String FLUSH_NOTIFICATION_COUNTER = SERVICE_NAME + ".flush_notification";
    public static final String CDC_EVENT_WRITTEN_TO_BUFFER_COUNTER = SERVICE_NAME + ".cdc_event_written_to_buffer";
    public static final String CDC_FILES_WRITTEN_TO_COUNTER = SERVICE_NAME + ".cdc_file_written";

    public static final String SOURCE_INSTANCE_NAME = "source_instance_name";
    public static final String SOURCE_DATABASE_NAME = "source_database_name";
    public static final String SOURCE_SCHEMA_NAME = "source_schema_name";
    public static final String SOURCE_TABLE_NAME = "source_table_name";

    private final ObservationRegistry observationRegistry;
    private final MeterRegistry meterRegistry;

    public Observability(ObservationRegistry observationRegistry, MeterRegistry meterRegistry) {
        this.observationRegistry = observationRegistry;
        this.meterRegistry = meterRegistry;
    }

    public static String spanName(String prefix, String operation) {
        return prefix + "." + operation;
    }

    public static String metricName(String prefix, String operation) {
        return prefix + "." + operation + ".records";
    }

    public static String spanName(String operation) {
        return spanName(SERVICE_NAME, operation);
    }

    public static String metricName(String operation) {
        return metricName(SERVICE_NAME, operation);
    }

    public static Observability noop() {
        return new Observability(ObservationRegistry.create(), new SimpleMeterRegistry());
    }

    /**
     * Labels for counters and timing histograms. Blank values become {@code unknown}
     * so a meter is never registered with a null tag.
     */
    public static Map<String, String> sourceTags(
            String instanceName, String databaseName, String schemaName, String tableName) {
        return Map.of(
                SOURCE_INSTANCE_NAME, label(instanceName),
                SOURCE_DATABASE_NAME, label(databaseName),
                SOURCE_SCHEMA_NAME, label(schemaName),
                SOURCE_TABLE_NAME, label(tableName));
    }

    public static String label(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }

    public void tagSource(String instanceName, String databaseName, String schemaName, String tableName) {
        lowCardinalityTag(SOURCE_INSTANCE_NAME, label(instanceName));
        lowCardinalityTag(SOURCE_DATABASE_NAME, label(databaseName));
        lowCardinalityTag(SOURCE_SCHEMA_NAME, label(schemaName));
        lowCardinalityTag(SOURCE_TABLE_NAME, label(tableName));
    }

    public String observeOperation(String prefix, String operation, Map<String, String> tags, Supplier<String> action) {
        return observeOperation(prefix, operation, tags, action, null);
    }

    public String observeOperation(
            String prefix,
            String operation,
            Map<String, String> tags,
            Supplier<String> action,
            Function<RuntimeException, String> exceptionOutcome) {
        String outcome = "error";
        Observation observation = Observation.createNotStarted(spanName(prefix, operation), observationRegistry);
        if (tags != null) {
            tags.forEach(observation::lowCardinalityKeyValue);
        }
        long startedAtNanos = System.nanoTime();
        observation.start();
        try (Observation.Scope scope = observation.openScope()) {
            outcome = action.get();
            return outcome;
        } catch (RuntimeException ex) {
            if (exceptionOutcome != null) {
                outcome = exceptionOutcome.apply(ex);
            }
            if ("error".equals(outcome)) {
                observation.error(ex);
            }
            throw ex;
        } finally {
            recordOutcome(prefix, operation, observation, tags, outcome, startedAtNanos);
            observation.stop();
        }
    }

    public void observeOperationVoid(String prefix, String operation, Map<String, String> tags, Supplier<String> action) {
        observeOperationVoid(prefix, operation, tags, action, null);
    }

    public void observeOperationVoid(
            String prefix,
            String operation,
            Map<String, String> tags,
            Supplier<String> action,
            Function<RuntimeException, String> exceptionOutcome) {
        observeOperation(prefix, operation, tags, action, exceptionOutcome);
    }

    public void observeOperationVoid(String prefix, String operation, Supplier<String> action) {
        observeOperationVoid(prefix, operation, Collections.emptyMap(), action);
    }

    public <T> T observeCallable(String prefix, String operation, Map<String, String> tags, ThrowingSupplier<T> action)
            throws Exception {
        String outcome = "error";
        Observation observation = Observation.createNotStarted(spanName(prefix, operation), observationRegistry);
        if (tags != null) {
            tags.forEach(observation::lowCardinalityKeyValue);
        }
        long startedAtNanos = System.nanoTime();
        observation.start();
        try (Observation.Scope scope = observation.openScope()) {
            T result = action.get();
            outcome = "success";
            return result;
        } catch (Exception ex) {
            observation.error(ex);
            throw ex;
        } finally {
            recordOutcome(prefix, operation, observation, tags, outcome, startedAtNanos);
            observation.stop();
        }
    }

    public void observeCallableVoid(String prefix, String operation, Map<String, String> tags, ThrowingSupplier<String> action)
            throws Exception {
        observeCallable(prefix, operation, tags, () -> {
            action.get();
            return null;
        });
    }

    public String observeAccept(String operation, Map<String, String> tags, Supplier<String> action) {
        return observeOperation(SERVICE_NAME, operation, tags, action);
    }

    public void observeAcceptVoid(String operation, Map<String, String> tags, Supplier<String> action) {
        observeAcceptVoid(operation, tags, action, null);
    }

    public void observeAcceptVoid(String operation, Supplier<String> action) {
        observeAcceptVoid(operation, Collections.emptyMap(), action, null);
    }

    public void observeAcceptVoid(
            String operation,
            Supplier<String> action,
            Function<RuntimeException, String> exceptionOutcome) {
        observeAcceptVoid(operation, Collections.emptyMap(), action, exceptionOutcome);
    }

    public void observeAcceptVoid(
            String operation,
            Map<String, String> tags,
            Supplier<String> action,
            Function<RuntimeException, String> exceptionOutcome) {
        observeOperationVoid(SERVICE_NAME, operation, tags, action, exceptionOutcome);
    }

    public void lowCardinalityTag(String key, String value) {
        Observation current = observationRegistry.getCurrentObservation();
        if (current != null) {
            current.lowCardinalityKeyValue(key, value);
        }
    }

    public void incrementCounter(String metricName, String tagKey, String tagValue) {
        meterRegistry.counter(metricName, tagKey, tagValue).increment();
    }

    public void incrementCounter(String metricName, String... tags) {
        meterRegistry.counter(metricName, tags).increment();
    }

    public void incrementCounter(String metricName, String tagKey, String tagValue, int inc) {
        meterRegistry.counter(metricName, tagKey, tagValue).increment(inc);
    }

    /**
     * Counter and timing histogram share the span's low-cardinality tags, including source
     * labels added before this call returns and the outcome tag.
     */
    private void recordOutcome(
            String prefix,
            String operation,
            Observation observation,
            Map<String, String> tags,
            String outcome,
            long startedAtNanos) {
        observation.lowCardinalityKeyValue("outcome", label(outcome));
        String[] meterTags = meterTags(observation, tags, outcome);
        meterRegistry.counter(metricName(prefix, operation), meterTags).increment();
        Timer.builder(spanName(prefix, operation))
                .tags(meterTags)
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(System.nanoTime() - startedAtNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * A registry with no handlers treats observations as no-ops, so tags set on the span
     * are not stored. Merge the caller's tags with whatever the span retained, including
     * source labels added during the operation.
     */
    private static String[] meterTags(Observation observation, Map<String, String> tags, String outcome) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (tags != null) {
            tags.forEach((key, value) -> merged.put(key, label(value)));
        }
        observation.getContext().getLowCardinalityKeyValues().forEach(keyValue ->
                merged.put(keyValue.getKey(), label(keyValue.getValue())));
        merged.put("outcome", label(outcome));
        List<String> pairs = new ArrayList<>(merged.size() * 2);
        merged.forEach((key, value) -> {
            pairs.add(key);
            pairs.add(value);
        });
        return pairs.toArray(String[]::new);
    }
}
