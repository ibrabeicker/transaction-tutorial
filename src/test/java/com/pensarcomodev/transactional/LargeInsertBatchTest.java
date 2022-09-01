package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.CompanyNoIdGeneration;
import com.pensarcomodev.transactional.repository.CompanyBatchRepository;
import com.pensarcomodev.transactional.repository.CompanyNoIdGenerationRepository;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.service.LargeReadService;
import com.pensarcomodev.transactional.util.HibernateUtils;
import com.pensarcomodev.transactional.util.TimeMetric;
import org.hibernate.Session;
import org.hibernate.stat.SessionStatistics;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@TestPropertySource(properties={
        "spring.jpa.properties.hibernate.show_sql=false",
        "spring.jpa.properties.hibernate.jdbc.batch_size=50",
        "spring.jpa.properties.hibernate.order_inserts=true",
        "spring.jpa.properties.hibernate.generate_statistics=true",
        "logging.level.org.springframework.jdbc.core=TRACE"
})
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
public class LargeInsertBatchTest {

    @Autowired CompanyRepository companyRepository;
    @Autowired CompanyNoIdGenerationRepository companyNoIdGenerationRepository;
    @Autowired CompanyBatchRepository companyBatchRepository;

    private static final Logger log = LoggerFactory.getLogger(LargeInsertBatchTest.class);

    /**
     * O Hibernate desativa silenciosamente a persistência em batch para entidades anotadas com @GeneratedValue(strategy = GenerationType.IDENTITY) ,
     * como pode ser visto no log das estatísticas "0 nanoseconds spent executing 0 JDBC batches;"
     */
    @Test
    public void insertEntityWithIdGeneration_doesNotUseBatch() {
        companyRepository.deleteAllInBatch();
        List<Company> toPersist = new ArrayList<>();
        TimeMetric timeMetric = new TimeMetric();
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
        log.info("Took {} ms", timeMetric.getDuration());
    }

    /**
     * Para que a persistência em batch funcione são necessários os seguintes requisitos:
     * - Propriedade definida "spring.jpa.properties.hibernate.jdbc.batch_size=50"
     * - Propriedade definida "spring.jpa.properties.hibernate.order_inserts=true"
     * - A entidade tenha um GenerationType SEQUENCE ou AUTO
     *
     * Como pode ser visto na mensagem de log "10782059 nanoseconds spent executing 1 JDBC statements;" a inserção dessa
     * forma é feita em batch.
     */
    @Test
    public void insertEntityWithoutIdGeneration_usesBatch() {
        companyRepository.deleteAllInBatch();
        List<CompanyNoIdGeneration> toPersist = new ArrayList<>();
        TimeMetric timeMetric = new TimeMetric();
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
        log.info("Took {} ms", timeMetric.getDuration());
    }

    /**
     * Quando for necessário apenas persistir um grande volume de dados o JdbcTemplate contorna as restrições dos cenários anteriores.
     */
    @Test
    public void insertUsingJdbcTemplate() {
        companyRepository.deleteAllInBatch();
        TimeMetric timeMetric = new TimeMetric();
        List<Company> companies = IntStream.rangeClosed(1, 1000)
                .mapToObj(i -> Company.builder()
                        .id((long) i + 100000)
                        .document(String.format("%014d", i))
                        .build())
                .collect(Collectors.toList());
        companyBatchRepository.saveAll(companies);
        log.info("Took {} ms", timeMetric.getDuration());
    }

}
