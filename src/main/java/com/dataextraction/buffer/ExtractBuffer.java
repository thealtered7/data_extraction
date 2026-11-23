package com.dataextraction.buffer;

import java.io.Closeable;
import java.io.IOException;

import com.dataextraction.cdc_consumer.RecordDelegate;
import com.dataextraction.sink.ExtractOutputFormat;

/**
 * One table's buffered extract file. Implementations differ by incoming change format.
 */
public interface ExtractBuffer extends Closeable {

    int write(RecordDelegate record) throws IOException;

    void flush() throws IOException;

    ExtractOutputFormat outputFormat();
}
