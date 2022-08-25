package com.pensarcomodev.transactional.concurrency;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Semaphore;

@Slf4j
public class PingPongLock {

    private final Semaphore pingLock;
    private final Semaphore pongLock;


    public PingPongLock() {
        this.pingLock = new Semaphore(1);
        this.pongLock = new Semaphore(1);
        this.pingLock.acquireUninterruptibly();
        this.pongLock.acquireUninterruptibly();
    }

    public void ping() {
        log.info("Ping");
        pingLock.release();
        pongLock.acquireUninterruptibly();
        log.info("Received Pong");
    }

    public void waitPing() {
        //this.pongLock.acquireUninterruptibly();
        this.pingLock.acquireUninterruptibly();
    }

    public void pong() {
        log.info("Pong");
        this.pongLock.release();
        this.pingLock.acquireUninterruptibly();
        log.info("Received ping");
    }

    public void waitPong() {
        this.pongLock.acquireUninterruptibly();
        this.pongLock.acquireUninterruptibly();
    }

    public void end() {
        this.pongLock.release();
        this.pingLock.release();
    }

    public void asyncEnd(int millis) {
        new Thread(() -> {
            try {
                Thread.sleep(millis);
                end();
            } catch (InterruptedException e) {
                //
            }
        }).start();
    }
}

