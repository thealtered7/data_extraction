package com.dataextraction.cdc_consumer.format;

public final class StartedSnapshot {

    private final Runnable engineTask;
    private final AutoCloseable engine;
    private final AutoCloseable consumer;

    public StartedSnapshot(Runnable engineTask, AutoCloseable engine, AutoCloseable consumer) {
        this.engineTask = engineTask;
        this.engine = engine;
        this.consumer = consumer;
    }

    public Runnable engineTask() {
        return engineTask;
    }

    public AutoCloseable engine() {
        return engine;
    }

    public AutoCloseable consumer() {
        return consumer;
    }
}
