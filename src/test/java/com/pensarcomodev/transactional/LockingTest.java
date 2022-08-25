package com.pensarcomodev.transactional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@Testcontainers
@ActiveProfiles("test")
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
public class LockingTest extends AbstractTest {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TransactionTest.class);

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

    @Test
    public void testNotLockingRootEntity() {

        thread1 = new Thread(() -> companyService.fireEveryone(companyId, countDownLatch, firedFirst));
        thread2 = new Thread(() -> companyService.fireEveryone(companyId, countDownLatch, firedLast));
        startThreads();
        assertDeletions();
    }

    @Test
    public void testNotLockingRootEntityFlush() {

        thread1 = new Thread(() -> companyService.fireEveryoneBatch(companyId, countDownLatch, firedFirst));
        thread2 = new Thread(() -> companyService.fireEveryoneBatch(companyId, countDownLatch, firedLast));
        startThreads();
        assertDeletions();
    }

    @Test
    public void testPessimisticWrite() {

        thread1 = new Thread(() -> companyService.fireEveryonePessimisticWrite(companyId, countDownLatch, firedFirst));
        thread2 = new Thread(() -> companyService.fireEveryonePessimisticWrite(companyId, countDownLatch, firedLast));
        startThreads();
        assertDeletions();
    }

    @Test
    public void testPessimisticRead() {

        thread1 = new Thread(() -> companyService.fireEveryonePessimisticRead(companyId, countDownLatch, firedFirst));
        thread2 = new Thread(() -> companyService.fireEveryonePessimisticRead(companyId, countDownLatch, firedLast));
        startThreads();
        assertDeletions();
    }

    private void startThreads() {
        thread1.start();
        thread2.start();
        countDownLatch.countDown();
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

}
