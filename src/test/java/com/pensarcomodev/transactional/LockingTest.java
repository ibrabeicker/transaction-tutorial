package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.concurrency.ParallelTransactions;
import com.pensarcomodev.transactional.concurrency.SequenceLock;
import com.pensarcomodev.transactional.entity.Company;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
public class LockingTest extends AbstractTest {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LockingTest.class);

    private List<String> firedFirst;
    private List<String> firedLast;
    private Thread thread1;
    private Thread thread2;
    private Long companyId;
    private CountDownLatch countDownLatch;

    @BeforeEach
    public void setUpLockingTest() {
        setUp();
        firedFirst = new ArrayList<>();
        firedLast = new ArrayList<>();
        company = companyRepository.save(company);
        companyId = company.getId();
        persistEmployees(4);
        countDownLatch = new CountDownLatch(1);
    }

    /**
     * Em duas transações paralelas, tenta remover todos os funcionário de uma empresa
     *
     * Empresa criada no banco com 4 funcionários
     * Em duas transações idênticas, faz o select da empresa pelo id e faz o select de todos os funcionários
     * Chama o delete para todos os funcionários retornados no passo anterior
     *
     * Resultado esperado:
     * Apenas uma das transações remova todos os funcionários e a outra não tenha nada para deletar
     *
     * Resultado obtido:
     * As entidades dos funcionários ficam sendo gerenciadas pelo EntityManager, por isso na segunda chamada ao delete
     * acontece um erro de tentativa de deleção de entidades que não existem mais
     */
    @Test
    public void testNotLockingRootEntity() {

        runSimultaneouslly(
                () -> companyService.fireEveryone(companyId, countDownLatch, firedFirst),
                () -> companyService.fireEveryone(companyId, countDownLatch, firedLast));
        assertDeletions();
    }

    /**
     * Mesmo do cenário anterior, com a chamada de deleção em batch
     *
     * Resultado esperado:
     * Apenas uma das transações remova todos os funcionários e a outra não tenha nada para deletar
     *
     * Resultado obtido:
     * Ambas as transações obtém uma lista com todos os funcionários a serem deletados, causando duplicidade na regra
     */
    @Test
    public void testNotLockingRootEntityFlush() {

        runSimultaneouslly(
                () -> companyService.fireEveryoneBatch(companyId, countDownLatch, firedFirst),
                () -> companyService.fireEveryoneBatch(companyId, countDownLatch, firedLast));
        assertDeletions();
    }

    /**
     * Mesmo do cenário anterior, com a chamada ao select da empresa usando um lock PESSIMISTIC_WRITE
     *
     * Resultado esperado:
     * Apenas uma das transações remova todos os funcionários e a outra não tenha nada para deletar
     *
     * Resultado obtido:
     * Ambas as transações requerem um select com PESSIMISTIC_WRITE na entidade da empresa antes de deletar os funcionários,
     * pela presença do lock apenas uma delas consegue o resultado para o select enquanto a outra fica esperando o lock
     * ser liberado ao final da transação da primeira. A primeira transação então deleta todos os funcionários e comita,
     * liberando o select com PESSIMISTIC_WRITE da segunda, que nesse momento obtém um resultado vazio no
     * select dos funcionários, pois agora estão todos removidos do banco.
     */
    @Test
    public void testPessimisticWrite() {

        runSimultaneouslly(
                () -> companyService.fireEveryonePessimisticWrite(companyId, countDownLatch, firedFirst),
                () -> companyService.fireEveryonePessimisticWrite(companyId, countDownLatch, firedLast));
        assertDeletions();
    }

    /**
     * Mesmo do cenário anterior, com a chamada ao select da empresa usando um lock PESSIMISTIC_READ
     *
     * Resultado esperado:
     * Apenas uma das transações remova todos os funcionários e a outra não tenha nada para deletar
     *
     * Resultado obtido:
     * PESSIMISTIC_READ permite a aquisição simultânea, portanto as transações executam a deleção em duplicidade
     */
    @Test
    public void testPessimisticRead() {

        runSimultaneouslly(
                () -> companyService.fireEveryonePessimisticRead(companyId, countDownLatch, firedFirst),
                () -> companyService.fireEveryonePessimisticRead(companyId, countDownLatch, firedLast));
        assertDeletions();
    }

    /**
     * PESSIMISTIC_READ e PESSIMISTIC_WRITE são mutualmente exclusivos, portante quando um é adquirido o outro é bloqueado
     * esperando o fim da transação
     */
    @Test
    public void testPessimisticReadWithPessimisticWriteSequence() {

        AtomicInteger count = new AtomicInteger();
        List<String> firedDocuments = new ArrayList<>();
        SequenceLock sequenceLock = new SequenceLock();
        ParallelTransactions.builder()
                .action1(() -> {
                    Company company = companyRepository.findByIdPessimisticRead(companyId);
                    count.set(employeeService.findByCompany(company).size());
                })
                .expectBlock2(() -> {
                    log.info("Deleting with pessimistic write");
                    Company company = companyRepository.findByIdPessimisticWrite(companyId);
                    sequenceLock.expectBlocking();
                    log.info("Locked company with PESSIMISTIC_WRITE");
                    firedDocuments.addAll(employeeService.deleteAll(company));
                })
                .action1(() -> {
                    log.info("Continuing transaction 1");
                    sequenceLock.expectRunFirst();
                })
                .execute(transactionService::transactional, transactionService::transactional);

        assertEquals(4, count.get());
        assertEquals(4, firedDocuments.size());
        assertTrue(sequenceLock.isRightOrder());
    }

    /**
     * Mesmo do cenário anterior, com a transação do PESSIMISTIC_WRITE executando primeiro
     */
    @Test
    public void testPessimisticWriteWithPessimisticReadSequence() {

        AtomicInteger count = new AtomicInteger();
        List<String> firedDocuments = new ArrayList<>();
        SequenceLock sequenceLock = new SequenceLock();
        ParallelTransactions.builder()
                .action1(() -> {
                    Company company = companyRepository.findByIdPessimisticWrite(companyId);
                    firedDocuments.addAll(employeeService.deleteAll(company));
                })
                .expectBlock2(() -> {
                    log.info("Reading with pessimistic read");
                    Company company = companyRepository.findByIdPessimisticRead(companyId);
                    sequenceLock.expectBlocking();
                    log.info("Locked company with PESSIMISTIC_READ");
                    count.set(employeeService.findByCompany(company).size());
                })
                .action1(() -> {
                    log.info("Continuing transaction 1");
                    sequenceLock.expectRunFirst();
                })
                .execute(transactionService::transactional, transactionService::transactional);

        assertEquals(0, count.get());
        assertEquals(4, firedDocuments.size());
        assertTrue(sequenceLock.isRightOrder());
    }

    private void startThreads() {
        startThreads(true);
    }

    private void startThreads(boolean countDown) {
        thread1.start();
        thread2.start();
        if (countDown) {
            countDownLatch.countDown();
        }
        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertDeletions() {
        int firedFirstSize = firedFirst.size();
        int firedLastSize = firedLast.size();
        assertTrue((firedFirstSize == 4 && firedLastSize == 0) || (firedFirstSize == 0 && firedLastSize == 4),
                String.format("Unexpected firedFirst=%d firedLast=%d", firedFirstSize, firedLastSize));
    }

    private void runSimultaneouslly(Runnable runnable1, Runnable runnable2) {
        this.thread1 = new Thread(runnable1);
        this.thread2 = new Thread(runnable2);
        startThreads();
    }

}
