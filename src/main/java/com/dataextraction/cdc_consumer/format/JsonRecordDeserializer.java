package com.dataextraction.cdc_consumer.format;

/**
 * JSON events are already the Connect envelope string the rest of the JSON path parses.
 * This deserializer returns that string unchanged.
 */
public final class JsonRecordDeserializer implements RecordDeserializer<String> {

    public static JsonRecordDeserializer create() {
        return new JsonRecordDeserializer();
    }

    private JsonRecordDeserializer() {
    }

    @Override
    public DeserializedRecord deserialize(String destination, String key, String value) {
        return DecodedChange.passthrough(key, value);
    }
}
