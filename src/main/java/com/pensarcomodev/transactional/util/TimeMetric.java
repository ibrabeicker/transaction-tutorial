package com.pensarcomodev.transactional.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimeMetric {

    private final long start;
    private long end = -1L;

    public TimeMetric() {
        this.start = System.currentTimeMillis();
    }

    public long getDuration() {
        if (end == -1L) {
            end = System.currentTimeMillis();
        }
        return end - start;
    }
}
