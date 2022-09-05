package com.pensarcomodev.transactional.concurrency;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Semaphore;

/**
 * Mecanismo para facilitar a compreensão da execução de duas threads paralelas.
 *
 * O lock começa com o a thread T1 executando e a thread T2 aguardando o envio do ping pela T1. Na chamada de ping() pela
 * T1 o cenário inverte, fazendo a T1 suspender sua execução e a T2 recebendo a instrução para executar até a chamada de
 * pong() e assim sucessivamente.
 */
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

    public void pong() {
        log.info("Pong");
        this.pongLock.release();
        this.pingLock.acquireUninterruptibly();
        log.info("Received ping");
    }

    public void waitPing() {
        this.pingLock.acquireUninterruptibly();
    }

    public void waitPong() {
        this.pongLock.acquireUninterruptibly();
    }

    public void sendPing() {
        this.pingLock.release();
    }

    public void sendPong() {
        this.pongLock.release();
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

