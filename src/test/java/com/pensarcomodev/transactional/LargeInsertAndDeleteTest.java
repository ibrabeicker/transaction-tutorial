package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.util.TimeMetric;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@TestPropertySource(properties={
        "spring.jpa.properties.hibernate.show_sql=false",
        "spring.jpa.properties.hibernate.generate_statistics=true"
})
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LargeInsertAndDeleteTest {

    @Autowired CompanyRepository companyRepository;

    private static long deleteAllTime = 1000;
    private static long deleteAllQueryTime = 2000;

    private static final Logger log = LoggerFactory.getLogger(LargeInsertAndDeleteTest.class);
    private final int TOTAL_SIZE = 10000;

    /**
     * Fazer inserção de N registross um a um em loop cria N transações para fazer cada insert, como mostrado pelo log.
     */
    @Test
    @Order(1)
    public void testInsertInLoop() {
        IntStream.rangeClosed(1, TOTAL_SIZE)
                .mapToObj(i -> Company.builder()
                        .document(String.format("%014d", i))
                        .build())
                .forEach(i -> companyRepository.save(i));
    }

    /**
     * O método deleteAll do repositório faz o select de todas as entidades e chama o delete de um por um, como pode ser
     * visto na implementação de SimpleJpaRepository.deleteAll().
     */
    @Test
    @Order(2)
    public void testDeleteByDeleteAll() {
        TimeMetric timeMetric = new TimeMetric();
        companyRepository.deleteAll();
        deleteAllTime = timeMetric.getDuration();
    }

    /**
     * Ao contrário do insert em loop, o método saveAll usa uma transação em cada chamada, reduzindo o número de transações
     * necessárias para inserir todos os registros, que se traduz num tempo de execução menor.
     */
    @Test
    @Order(3)
    public void testInsertSaveAll() {
        List<Company> toPersist = new ArrayList<>();
        IntStream.rangeClosed(1, TOTAL_SIZE)
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

    /**
     * Para evitar o comportamento de ter que carregar todas as entidades que desejamos deletar e chamar o delete de cada
     * uma individualmente podemos chamar deleteAllInBatch() que executa apenas uma query de deleção.
     */
    @Test
    @Order(4)
    public void testDeleteAllInBatch() {
        TimeMetric timeMetric = new TimeMetric();
        companyRepository.deleteAllInBatch();
        deleteAllQueryTime = timeMetric.getDuration();
    }

    /**
     * Como esperado, o tempo de executar apenas uma query para deletar todos os registros é muito mais rápida.
     */
    @Test
    @Order(5)
    public void testDeleteAllInBatchIsFaster() {
        log.info("deleteAllQueryTime took {} ms, deleteAllTime took {} ms", deleteAllQueryTime, deleteAllTime);
        assertTrue(deleteAllQueryTime < deleteAllTime);
    }

}
