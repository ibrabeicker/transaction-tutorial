package com.pensarcomodev.transactional.concurrency;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class ParallelTransactions {
    private final List<Runnable> transactionActions1 = new ArrayList<>();
    private final List<Runnable> transactionActions2 = new ArrayList<>();
    private Thread thread1;
    private Thread thread2;

    private final PingPongLock lock = new PingPongLock();
    private final Runnable waitPing = lock::waitPing;
    private final Runnable ping = lock::ping;
    private final Runnable pong = lock::pong;
    private int lastTransaction = 0;

    private final ScheduledExecutorService executor;

    private ParallelTransactions() {
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    public static ParallelTransactions builder() {
        return new ParallelTransactions();
    }

    public ParallelTransactions action1(Runnable action) {
        checkTransaction1();
        this.transactionActions1.add(action);
        this.transactionActions1.add(ping);
        return this;
    }

    public ParallelTransactions action2(Runnable action) {
        checkTransaction2();
        this.transactionActions2.add(action);
        this.transactionActions2.add(pong);
        return this;
    }

    public ParallelTransactions action1(Consumer<ParallelTransactions.LockDecorator> action) {
        checkTransaction1();
        Runnable r = () -> action.accept(new ParallelTransactions.LockDecorator(lock, 1));
        this.transactionActions1.add(r);
        this.transactionActions1.add(lock::waitPong);
        return this;
    }

    public ParallelTransactions action2(Consumer<ParallelTransactions.LockDecorator> action) {
        checkTransaction2();
        Runnable r = () -> action.accept(new ParallelTransactions.LockDecorator(lock, 2));
        this.transactionActions2.add(r);
        this.transactionActions2.add(lock::waitPing);
        return this;
    }

    public ParallelTransactions expectBlock1(Runnable action) {
        Runnable checkBlock = () -> executor.schedule(lock::sendPing, 100, TimeUnit.MILLISECONDS);
        this.transactionActions1.add(checkBlock);
        this.transactionActions1.add(action);
        this.transactionActions1.add(lock::waitPong);
        this.lastTransaction = 1;
        return this;
    }

    public ParallelTransactions expectBlock2(Runnable action) {
        Runnable checkBlock = () -> executor.schedule(lock::sendPong, 100, TimeUnit.MILLISECONDS);
        this.transactionActions2.add(checkBlock);
        this.transactionActions2.add(action);
        this.transactionActions2.add(lock::waitPing);
        this.lastTransaction = 2;
        return this;
    }

    @SneakyThrows
    public void execute(Consumer<List<Runnable>> method1, Consumer<List<Runnable>> method2) {
        transactionActions2.add(0, waitPing);
        transactionActions1.remove(transactionActions1.size() - 1);
        transactionActions2.remove(transactionActions2.size() - 1);
        thread1 = new Thread(null, () -> {
            log.info("Starting transaction 1");
            method1.accept(transactionActions1);
            log.info("Commiting transaction 1");
            lock.end();
        }, "Transaction-1", 0);
        thread2 = new Thread(null, () -> {
            log.info("Starting transaction 2");
            method2.accept(transactionActions2);
            log.info("Commiting transaction 2");
            lock.end();
        }, "Transaction-2", 0);
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
    }

    private void checkTransaction1() {
        if (lastTransaction == 1) {
            throw new IllegalStateException("action1 must be preceded by action2");
        }
        lastTransaction = 1;
    }

    private void checkTransaction2() {
        if (lastTransaction == 0) {
            throw new IllegalStateException("action1 must be called first");
        }
        if (lastTransaction == 2) {
            throw new IllegalStateException("action2 must be preceded by action1");
        }
        lastTransaction = 2;
    }

    @SneakyThrows
    public static void sleep(long millis) {
        Thread.sleep(millis);
    }

    public static class LockDecorator {

        private final PingPongLock lock;
        private final int transaction;

        public LockDecorator(PingPongLock lock, int transaction) {
            this.lock = lock;
            this.transaction = transaction;
        }

        public void next() {
            if (transaction == 1) {
                lock.sendPing();
            } else {
                lock.sendPong();
            }
        }

    }

}
