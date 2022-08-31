package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.CompanyNoIdGeneration;
import com.pensarcomodev.transactional.repository.CompanyNoIdGenerationRepository;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.service.LargeReadService;
import com.pensarcomodev.transactional.util.HibernateUtils;
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

import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@TestPropertySource(properties={
        "spring.jpa.properties.hibernate.show_sql=false",
        "spring.jpa.properties.hibernate.jdbc.batch_size=50",
        "spring.jpa.properties.hibernate.order_inserts=true",
        "spring.jpa.properties.hibernate.generate_statistics=true"
})
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
public class LargeInsertBatchTest {

    @Autowired CompanyRepository companyRepository;
    @Autowired CompanyNoIdGenerationRepository companyNoIdGenerationRepository;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TransactionTest.class);

    @Test
    public void testInsertWithBatch_entityWithIdGeneration_doesNotUseBatch() {
        companyRepository.deleteAllQuery();
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
    public void testInsertWithBatch_entityWithoutIdGeneration_usesBatch() {
        companyRepository.deleteAllQuery();
        List<CompanyNoIdGeneration> toPersist = new ArrayList<>();
        IntStream.rangeClosed(1, 1000)
                .mapToObj(i -> CompanyNoIdGeneration.builder()
                        .id((long) i + 100000)
                        .document(String.format("%014d", i))
                        .build())
                .forEach(c -> {
                    toPersist.add(c);
                    if (toPersist.size() == 500) {
                        companyNoIdGenerationRepository.saveAll(toPersist);
                        toPersist.clear();
                    }
                });
    }

}
