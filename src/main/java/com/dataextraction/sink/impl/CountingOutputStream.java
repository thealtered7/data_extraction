package com.dataextraction.sink.impl;

import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;

public class CountingOutputStream extends OutputStream {
    private final OutputStream delegate;
    private final AtomicLong count = new AtomicLong();

    public CountingOutputStream(OutputStream delegate) {
        this.delegate = delegate;
    }

    long bytesWritten() {
        return count.get();
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        count.incrementAndGet();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
        count.addAndGet(len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}