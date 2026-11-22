package com.dataextraction.sink;

/**
 * Creates a new {@link ExtractRecordSink} for one extract buffer.
 * A sink is bound to a single temp file, so each buffer needs its own instance.
 */
public interface ExtractRecordSinkFactory {

    ExtractRecordSink create();
}
