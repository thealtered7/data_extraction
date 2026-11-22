package com.dataextraction.sink.impl;

import com.dataextraction.sink.ExtractRecordSink;
import com.dataextraction.sink.ExtractRecordSinkFactory;

public final class JsonlExtractRecordSinkFactory implements ExtractRecordSinkFactory {

    @Override
    public ExtractRecordSink create() {
        return new JsonlExtractRecordSink();
    }
}
