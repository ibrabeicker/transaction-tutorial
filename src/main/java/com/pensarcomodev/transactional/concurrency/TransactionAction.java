package com.pensarcomodev.transactional.concurrency;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TransactionAction {

    private final Runnable action;
    private final int transaction;

}
