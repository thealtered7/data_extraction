package com.dataextraction.sink.impl;

import com.dataextraction.sink.ExtractRecordSink;
import com.dataextraction.sink.ExtractRecordSinkFactory;
import java.util.Objects;

public final class AvroExtractRecordSinkFactory implements ExtractRecordSinkFactory {

    private final ConnectAvroSchemas connectAvroSchemas;

    public AvroExtractRecordSinkFactory(ConnectAvroSchemas connectAvroSchemas) {
        this.connectAvroSchemas = Objects.requireNonNull(connectAvroSchemas, "connectAvroSchemas");
    }

    @Override
    public ExtractRecordSink create() {
        return new AvroExtractRecordSink(connectAvroSchemas);
    }
}
