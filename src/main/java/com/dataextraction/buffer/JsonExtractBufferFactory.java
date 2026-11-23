package com.dataextraction.buffer;

public final class JsonExtractBufferFactory implements ExtractBufferFactory {

    @Override
    public ExtractBuffer create(ExtractBufferRequest request) {
        return ExtractJsonBuffer.builder()
                .withTableName(request.getTableName())
                .withExtractType(request.getExtractType())
                .withExtractJobId(request.getExtractJobId())
                .withMaxBufferSize(request.getMaxBufferSize())
                .withObservability(request.getObservability())
                .withOnFlush(request.getOnFlush())
                .withInstanceName(request.getInstanceName())
                .withSchemaRegistrar(request.getSchemaRegistrar())
                .withSink(request.getSink())
                .withFlushedBufferHandler(request.getFlushedBufferHandler())
                .build();
    }
}
