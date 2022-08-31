package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.service.LargeReadService;
import com.pensarcomodev.transactional.util.HibernateUtils;
import com.pensarcomodev.transactional.util.TimeMetric;
import org.hibernate.Session;
import org.hibernate.stat.SessionStatistics;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@TestPropertySource(properties="spring.jpa.properties.hibernate.show_sql=false")
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LargeInsertAndDeleteTest {

    @Autowired CompanyRepository companyRepository;
    @Autowired EntityManager entityManager;

    private long deleteAllTime;
    private long deleteAllQueryTime;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TransactionTest.class);

    @Test
    @Order(1)
    public void testInsertInLoop() {
        IntStream.rangeClosed(1, 1000)
                .mapToObj(i -> Company.builder()
                        .document(String.format("%014d", i))
                        .build())
                .forEach(i -> companyRepository.save(i));
    }

    @Test
    @Order(2)
    public void testDeleteByDeleteAll() {
        TimeMetric timeMetric = new TimeMetric();
        companyRepository.deleteAll();
        deleteAllTime = timeMetric.getDuration();
    }

    @Test
    @Order(3)
    public void testInsertSaveAll() {
        List<Company> toPersist = new ArrayList<>();
        IntStream.rangeClosed(1, 1000)
                .mapToObj(i -> Company.builder()
                        .document(String.format("%014d", i))
                        .build())
                .forEach(c -> {
                    toPersist.add(c);
                    if (toPersist.size() == 500) {
                        companyRepository.saveAll(toPersist);
                        toPersist.clear();
                    }
            });
    }

    @Test
    @Order(4)
    public void testDeleteByQuery() {
        TimeMetric timeMetric = new TimeMetric();
        companyRepository.deleteAllQuery();
        deleteAllQueryTime = timeMetric.getDuration();
    }

    @Test
    @Order(5)
    public void testInsertSaveAllTransaction() {
        TimeMetric timeMetric = new TimeMetric();
        List<Company> toPersist = new ArrayList<>();
        IntStream.rangeClosed(1, 1000)
                .mapToObj(i -> Company.builder()
                        .document(String.format("%014d", i))
                        .build())
                .forEach(c -> {
                    toPersist.add(c);
                    if (toPersist.size() == 500) {
                        companyRepository.saveAllTransaction(toPersist);
                        toPersist.clear();
                    }
                });
        log.info("testInsertSaveAllTransaction in {} ms", timeMetric.getDuration());
    }

}
