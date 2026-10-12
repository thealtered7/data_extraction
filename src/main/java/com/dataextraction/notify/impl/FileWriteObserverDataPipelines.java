package com.dataextraction.notify.impl;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataextraction.notify.FileWrittenObserver;
import com.dataextraction.notify.events.FileWriteFlushEvent;

public class FileWriteObserverDataPipelines implements FileWrittenObserver {
    private static final Logger log = LoggerFactory.getLogger(FileWriteObserverDataPipelines.class);
    @Override
    public void observe(FileWriteFlushEvent event) {
        log.info("Observing file write flush event: {}", event);
        // TODO: Implement the logic to observe the file write flush event
    }
}
