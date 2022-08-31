package com.pensarcomodev.transactional.util;

public class TimeMetric {

    private final long start;
    private long end;

    public TimeMetric() {
        this.start = System.currentTimeMillis();
    }

    public long getDuration() {
        if (end == 0) {
            end = System.currentTimeMillis();
        }
        return end - start;
    }
}
