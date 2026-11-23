package com.dataextraction.cdc_consumer;

import com.dataextraction.cdc_consumer.format.DeserializedRecord;

public final class JsonRecordDelegateFactory implements RecordDelegateFactory {

    public static JsonRecordDelegateFactory create() {
        return new JsonRecordDelegateFactory();
    }

    private JsonRecordDelegateFactory() {
    }

    @Override
    public RecordDelegate create(String destination, DeserializedRecord record) {
        return JsonRecordDelegate.from(destination, record);
    }
}
