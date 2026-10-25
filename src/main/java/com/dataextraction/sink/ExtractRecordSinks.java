package com.dataextraction.sink;

import com.dataextraction.sink.impl.AvroExtractRecordSink;
import com.dataextraction.sink.impl.JsonlExtractRecordSink;

public final class ExtractRecordSinks {

    private ExtractRecordSinks() {}

    static ExtractRecordSink create(ExtractOutputFormat format) {
        return switch (ExtractOutputFormat.require(format)) {
            case JSONL -> new JsonlExtractRecordSink();
            case AVRO -> new AvroExtractRecordSink();
        };
    }
}
