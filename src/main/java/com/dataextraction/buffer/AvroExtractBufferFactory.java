package com.dataextraction.buffer;

public final class AvroExtractBufferFactory implements ExtractBufferFactory {

    @Override
    public ExtractBuffer create(ExtractBufferRequest request) {
        return ExtractAvroBuffer.builder()
                .withTableName(request.getTableName())
                .withExtractType(request.getExtractType())
                .withExtractJobId(request.getExtractJobId())
                .withMaxBufferSize(request.getMaxBufferSize())
                .withObservability(request.getObservability())
                .withOnFlush(request.getOnFlush())
                .withInstanceName(request.getInstanceName())
                .withSink(request.getSink())
                .withFlushedBufferHandler(request.getFlushedBufferHandler())
                .build();
    }
}
