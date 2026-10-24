package com.dataextraction.buffer;

import java.io.InputStream;

import com.dataextraction.notify.events.FileWriteFlushEvent;

public interface FlushedBufferHandler {
    public String handleFlushedBuffer(FileWriteFlushEvent event, InputStream readStream, long contentLength);
}