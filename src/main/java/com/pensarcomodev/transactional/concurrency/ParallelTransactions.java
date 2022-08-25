package com.pensarcomodev.transactional.concurrency;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ParallelTransactions {

    private List<TransactionAction> transactionActions = new ArrayList<>();

    private Consumer<List<Runnable>> transaction1Executor;
    private Consumer<List<Runnable>> transaction2Executor;
    private final PingPongLock lock = new PingPongLock();
    private final Runnable waitPing = lock::waitPing;
    private final Runnable ping = lock::ping;
    private final Runnable pong = lock::pong;
    private final Runnable end = lock::end;

    public ParallelTransactions(TransactionActionsRunner method1, TransactionActionsRunner method2) {
//        this.transaction1Executor = method1;
//        this.transaction2Executor = method2;
    }

    public ParallelTransactions action1(Runnable action) {
        if (!this.transactionActions.isEmpty() && this.transactionActions.get(this.transactionActions.size() - 1).getTransaction() == 1) {
            throw new IllegalStateException("action1 must be preceded by action2");
        }
        this.transactionActions.add(new TransactionAction(action, 1));
        return this;
    }

    public ParallelTransactions action2(Runnable action) {
        if (this.transactionActions.isEmpty()) {
            throw new IllegalStateException("action1 must be called first");
        }
        if (this.transactionActions.get(this.transactionActions.size() - 1).getTransaction() == 2) {
            throw new IllegalStateException("action2 must be preceded by action1");
        }
        this.transactionActions.add(new TransactionAction(action, 2));
        return this;
    }

    @SneakyThrows
    public void execute(Consumer<List<Runnable>> method1, Consumer<List<Runnable>> method2) {
        List<Runnable> runnables1 = new ArrayList<>();
        List<Runnable> runnables2 = new ArrayList<>();
        runnables2.add(waitPing);
        for (TransactionAction action : this.transactionActions) {
            if (action.getTransaction() == 1) {
                runnables1.add(action.getAction());
            } else {
                runnables2.add(action.getAction());
            }
            if (isLast(action)) {
                (action.getTransaction() == 1 ? runnables1 : runnables2).add(end);
            } else if (action.getTransaction() == 1) {
                runnables1.add(ping);
            } else {
                runnables2.add(pong);
            }
        }
        Thread thread1 = new Thread(() -> method1.accept(runnables1));
        Thread thread2 = new Thread(() -> method2.accept(runnables2));
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
    }

    private boolean isLast(TransactionAction action) {
        return this.transactionActions.indexOf(action) == this.transactionActions.size() - 1;
    }

}
