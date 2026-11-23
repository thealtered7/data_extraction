package com.dataextraction.cdc_consumer;

import com.dataextraction.cdc_consumer.format.DeserializedRecord;

public interface RecordDelegateFactory {

    RecordDelegate create(String destination, DeserializedRecord record);
}
