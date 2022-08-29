package com.pensarcomodev.transactional.concurrency;

import java.util.concurrent.atomic.AtomicBoolean;

public class SequenceLock {

    private final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    private boolean finalState = false;

    public void expectBlocking() {
        this.finalState = atomicBoolean.get();
    }

    public void expectRunFirst() {
        atomicBoolean.compareAndExchange(false, true);
    }

    public boolean isRightOrder() {
        return this.finalState;
    }
}
