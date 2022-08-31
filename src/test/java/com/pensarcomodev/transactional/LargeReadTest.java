package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.service.LargeReadService;
import com.pensarcomodev.transactional.util.TimeMetric;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@TestPropertySource(properties={
        "spring.jpa.properties.hibernate.show_sql=false"
})
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LargeReadTest {

    @Autowired LargeReadService largeReadService;
    @Autowired CompanyRepository companyRepository;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TransactionTest.class);

    private static final int PAGINATION_SIZE = 1000;
    private static final int TOTAL_SIZE = 100000;

    private long offsetPaginationDuration;
    private long indexPaginationDuration;

    @BeforeAll
    public void setup() {
        companyRepository.deleteAllQuery();
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
     * Fazer o select dentro de uma transação faz o entityManager gerenciar e segurar todas as entidades lidas em memória.
     */
    @Test
    public void testReadLargeDataset_withTransaction() {
        int managedEntities = largeReadService.selectAllOnTransaction();
        assertEquals(TOTAL_SIZE, managedEntities);
    }

    /**
     * Fazer o select sem uma transação faz com que o entityManager não guarde as entidades em memória, porém o resultado
     * ocupa a memória de todas as entidades selecionadas.
     */
    @Test
    public void testReadLargeDataset_withoutTransaction() {
        int managedEntities = largeReadService.selectAllWithoutTransactionWithTransaction();
        assertEquals(0, managedEntities);
    }

    /**
     * Obter um stream de resultado só pode ser feito dentro de um método transacional. Chamar ele sem uma transação causa
     * um erro.
     */
    @Test
    public void testStream_withoutTransaction() {
        List<Integer> managedEntities = largeReadService.selectAllWithStreamWithoutTransaction();
        assertTrue(managedEntities.get(0) < lastOf(managedEntities));
        assertEquals(TOTAL_SIZE, lastOf(managedEntities));
    }

    /**
     * Obter um stream de resultados em um select pode causar a impressão que as entidades não serão carregadas em sua
     * totalidade para a memória, porém isso não acontece. Como num select dentro de uma transação, todas as entidades
     * lidas ficam armazenadas pelo entityManager causando potencialmente um esgotamento da memória.
     */
    @Test
    public void testStream_withTransaction() {
        List<Integer> managedEntities = largeReadService.selectAllWithStreamWithTransaction();
        assertTrue(managedEntities.get(0) < lastOf(managedEntities));
        assertEquals(TOTAL_SIZE, lastOf(managedEntities));
    }

    /**
     * Usar o mecanismo de paginação do Spring é o método mais intuitivo para iterar sobre um conjunto de resultados
     * muito grande, porém é ineficiente a nível de banco para massas muito grandes.
     * A paginação do Spring funciona pelo comando de limit e offset, que indica ao banco o comando de "pule os X (offset)
     * primeiros registros e retorne os próximos Y (limit) resultados". Porém os bancos de dados só conseguem descobrir
     * esse offset iterando sobre X registros para descobrir onde está o começo dos resultados que estão sendo buscados.
     * Dessa forma o primeiro select com offset zero terá muito pouco esforço de descobrir onde está o registro de número zero,
     * mas na segunda será preciso iterar sobre todos os resultados obtidos no primeiro e retornar os Y seguintes, seguindo
     * dessa forma até chegar nas últimas páginas, quando o banco estará iterando sobre quase que a massa total de dados
     * para obter um conjunto comparativamente minúsculo de resultados.
     */
    @Test
    @Order(1)
    public void testSelect_usingOffsetPagination() {
        TimeMetric timeMetric = new TimeMetric();
        List<Integer> queryTimes = largeReadService.selectInBatchesWithOffsetPagination(PAGINATION_SIZE);
        offsetPaginationDuration = timeMetric.getDuration();
        log.info("First query time = {} ms", queryTimes.get(0));
        log.info("Last query time = {} ms", lastOf(queryTimes));
        assertTrue(queryTimes.get(0) < lastOf(queryTimes));
    }

    /**
     * Uma alternativa muito mais eficiente do que a abordagem anterior é fazer a paginação via índice, onde escolhemos uma
     * propriedade indexada da nossa tabela e fazemos o primeiro select de uma página ordenando por tal propriedade e
     * usando o id da última entidade retornada na página anterior como condição de corte para o próximo select, usando
     * da mesma ordenação. Como a propriedade está indexada o banco consegue usar seu mecanismo de índice para obter em
     * tempo muito mais eficiente o início de cada página.
     */
    @Test
    @Order(2)
    public void testSelect_usingIndexPagination_noTransaction() {
        TimeMetric timeMetric = new TimeMetric();
        List<Integer> managedEntities = largeReadService.selectIndexPaginationWithoutTransaction(false, PAGINATION_SIZE);
        indexPaginationDuration = timeMetric.getDuration();
        assertTrue(lastOf(managedEntities) < PAGINATION_SIZE);
        assertTrue(managedEntities.get(0) >= lastOf(managedEntities));
    }

    /**
     * Comparamos o tempo de execução de cada abordagem verificamos que a paginação via índice é mais rápida.
     */
    @Test
    @Order(3)
    public void testOffsetSlowerThanIndex() {
        assertNotEquals(0, indexPaginationDuration);
        assertNotEquals(0, offsetPaginationDuration);
        assertTrue(indexPaginationDuration < offsetPaginationDuration);
    }

    /**
     * Mesmo com a paginação via índice executando dentro de uma transação, sofremos com o comportamento do entityManager
     * armazenar todas as entidades em memória e potencialmente sofrer uma falta de recurso.
     */
    @Test
    public void testSelect_usingIndexPagination_withoutClear() {
        List<Integer> managedEntities = largeReadService.selectIndexPaginationWithTransaction(false, PAGINATION_SIZE);
        assertEquals(PAGINATION_SIZE, managedEntities.get(0));
        assertEquals(TOTAL_SIZE, lastOf(managedEntities));
    }

    /**
     * Caso seja necessário executar a paginação dentro de uma transação, deve-se chamar periodicamente entityManager.clear()
     * para liberar memória usada pelo entityManager.
     */
    @Test
    public void testSelect_usingIndexPagination_withClear() {
        List<Integer> managedEntities = largeReadService.selectIndexPaginationWithTransaction(true, PAGINATION_SIZE);
        assertEquals(PAGINATION_SIZE, managedEntities.get(0));
        assertEquals(PAGINATION_SIZE, lastOf(managedEntities));
    }

    private <T> T lastOf(List<T> entities) {
        return entities.get(entities.size() - 1);
    }

}
