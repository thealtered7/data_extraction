package com.dataextraction.buffer.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.dataextraction.notify.events.FileWriteFlushEvent;

class LocalFileStoreFlushedBufferHandlerTest {

    private static final Instant EXTRACT_START = Instant.parse("2026-03-05T14:30:00Z");

    @TempDir
    Path tempDir;

    @Test
    void writesStreamUnderUtcDatePath() throws IOException {
        LocalFileStoreFlushedBufferHandler handler =
                new LocalFileStoreFlushedBufferHandler(tempDir, "dev", ".JSON");
        byte[] payload = "row-1\nrow-2\n".getBytes();
        FileWriteFlushEvent event = event(EXTRACT_START);

        String writtenPath = handler.handleFlushedBuffer(event, new ByteArrayInputStream(payload), 999);

        Path expected = tempDir.resolve(Path.of(
                "dev",
                "prod-db",
                "geo",
                "public",
                "places",
                "2026",
                "03",
                "05",
                "job-1_buf-9_" + EXTRACT_START + ".json"));
        assertEquals(expected.toString(), writtenPath);
        assertArrayEquals(payload, Files.readAllBytes(expected));
    }

    @Test
    void usesUtcDateWhenLocalOffsetWouldBePreviousDay() throws IOException {
        Instant start = Instant.parse("2026-03-05T01:00:00Z");
        LocalFileStoreFlushedBufferHandler handler =
                new LocalFileStoreFlushedBufferHandler(tempDir, "dev", "jsonl");

        String writtenPath = handler.handleFlushedBuffer(event(start), new ByteArrayInputStream(new byte[0]), 0);

        assertEquals(
                tempDir.resolve(Path.of(
                        "dev", "prod-db", "geo", "public", "places", "2026", "03", "05",
                        "job-1_buf-9_" + start + ".jsonl")).toString(),
                writtenPath);
    }

    @Test
    void normalizesFileExtension() {
        assertEquals("json", new LocalFileStoreFlushedBufferHandler(tempDir, "dev", ".JSON").getFileExtension());
        assertEquals("csv", new LocalFileStoreFlushedBufferHandler(tempDir, "dev", "  CSV  ").getFileExtension());
        assertEquals("parquet", new LocalFileStoreFlushedBufferHandler(tempDir, "dev", "parquet").getFileExtension());
    }

    @Test
    void wrapsIoFailure() throws IOException {
        Path notADirectory = tempDir.resolve("not-a-directory");
        Files.writeString(notADirectory, "x");
        LocalFileStoreFlushedBufferHandler handler =
                new LocalFileStoreFlushedBufferHandler(notADirectory, "dev", "json");

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> handler.handleFlushedBuffer(
                        event(EXTRACT_START), new ByteArrayInputStream(new byte[] {1}), 1));

        assertInstanceOf(IOException.class, thrown.getCause());
    }

    private static FileWriteFlushEvent event(Instant extractStartAt) {
        return new FileWriteFlushEvent(
                "flush",
                Path.of("buffer.bin"),
                extractStartAt,
                extractStartAt,
                extractStartAt.plusSeconds(60),
                10,
                2,
                "prod-db",
                "geo",
                "public",
                "places",
                "job-1",
                "buf-9",
                "snapshot",
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
