package com.dataextraction.notify;

import com.dataextraction.notify.events.FileWriteFlushEvent;

public interface FileWrittenObserver {
    void observe(FileWriteFlushEvent event);    
}
