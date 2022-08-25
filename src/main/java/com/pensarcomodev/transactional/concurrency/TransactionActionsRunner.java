package com.pensarcomodev.transactional.concurrency;

import java.util.List;

public interface TransactionActionsRunner {

    void run(List<Runnable> runnables, PingPongLock lock);
}
