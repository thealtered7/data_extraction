package com.dataextraction.cdc_consumer;

import com.dataextraction.cdc_consumer.format.DeserializedRecord;

public final class AvroRecordDelegateFactory implements RecordDelegateFactory {

    public static AvroRecordDelegateFactory create() {
        return new AvroRecordDelegateFactory();
    }

    private AvroRecordDelegateFactory() {
    }

    @Override
    public RecordDelegate create(String destination, DeserializedRecord record) {
        return AvroRecordDelegate.from(destination, record);
    }
}
