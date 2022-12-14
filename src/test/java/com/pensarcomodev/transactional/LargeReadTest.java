package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.repository.CompanyBatchRepository;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.service.LargeReadService;
import com.pensarcomodev.transactional.util.TimeMetric;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;
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
    @Autowired CompanyBatchRepository companyBatchRepository;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LargeReadTest.class);

    private static final int PAGINATION_SIZE = 1000;
    private static final int TOTAL_SIZE = 100000;

    private long offsetPaginationDuration;
    private long indexPaginationDuration;

    @BeforeAll
    public void setup() {
        companyRepository.deleteAllInBatch();
        List<Company> toPersist = IntStream.rangeClosed(1, TOTAL_SIZE)
                .mapToObj(i -> Company.builder()
                        .document(String.format("%014d", i))
                        .build())
                .collect(Collectors.toList());
        companyBatchRepository.saveAll(toPersist);
    }

    /**
     * Fazer o select dentro de uma transa????o faz o entityManager gerenciar e segurar todas as entidades lidas em mem??ria.
     */
    @Test
    public void testReadLargeDataset_withTransaction() {
        int managedEntities = largeReadService.selectAllOnTransaction();
        assertEquals(TOTAL_SIZE, managedEntities);
    }

    /**
     * Fazer o select sem uma transa????o faz com que o entityManager n??o guarde as entidades em mem??ria, por??m o resultado
     * ocupa a mem??ria de todas as entidades selecionadas.
     */
    @Test
    public void testReadLargeDataset_withoutTransaction() {
        int managedEntities = largeReadService.selectAllWithoutTransactionWithTransaction();
        assertEquals(0, managedEntities);
    }

    /**
     * Obter um stream de resultado s?? pode ser feito dentro de um m??todo transacional. Chamar ele sem uma transa????o causa
     * um erro.
     */
    @Test
    public void testStream_withoutTransaction() {
        List<Integer> managedEntities = largeReadService.selectAllWithStreamWithoutTransaction();
        assertTrue(managedEntities.get(0) < lastOf(managedEntities));
        assertEquals(TOTAL_SIZE, lastOf(managedEntities));
    }

    /**
     * Obter um stream de resultados em um select pode causar a impress??o que as entidades n??o ser??o carregadas em sua
     * totalidade para a mem??ria, por??m isso n??o acontece. Como num select dentro de uma transa????o, todas as entidades
     * lidas ficam armazenadas pelo entityManager causando potencialmente um esgotamento da mem??ria.
     */
    @Test
    public void testStream_withTransaction() {
        List<Integer> managedEntities = largeReadService.selectAllWithStreamWithTransaction();
        assertTrue(managedEntities.get(0) < lastOf(managedEntities));
        assertTrue(lastOf(managedEntities) > TOTAL_SIZE * 0.9);
    }

    /**
     * Usar o mecanismo de pagina????o do Spring ?? o m??todo mais intuitivo para iterar sobre um conjunto de resultados
     * muito grande, por??m ?? ineficiente a n??vel de banco para massas muito grandes.
     * A pagina????o do Spring funciona pelo comando de limit e offset, que indica ao banco o comando de "pule os X (offset)
     * primeiros registros e retorne os pr??ximos Y (limit) resultados". Por??m os bancos de dados s?? conseguem descobrir
     * esse offset iterando sobre X registros para descobrir onde est?? o come??o dos resultados que est??o sendo buscados.
     * Dessa forma o primeiro select com offset zero ter?? muito pouco esfor??o de descobrir onde est?? o registro de n??mero zero,
     * mas na segunda ser?? preciso iterar sobre todos os resultados obtidos no primeiro e retornar os Y seguintes, seguindo
     * dessa forma at?? chegar nas ??ltimas p??ginas, quando o banco estar?? iterando sobre quase que a massa total de dados
     * para obter um conjunto comparativamente min??sculo de resultados.
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
     * Uma alternativa muito mais eficiente do que a abordagem anterior ?? fazer a pagina????o via ??ndice, onde escolhemos uma
     * propriedade indexada da nossa tabela e fazemos o primeiro select de uma p??gina ordenando por tal propriedade e
     * usando o id da ??ltima entidade retornada na p??gina anterior como condi????o de corte para o pr??ximo select, usando
     * da mesma ordena????o. Como a propriedade est?? indexada o banco consegue usar seu mecanismo de ??ndice para obter em
     * tempo muito mais eficiente o in??cio de cada p??gina.
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
     * Comparamos o tempo de execu????o de cada abordagem verificamos que a pagina????o via ??ndice ?? mais r??pida.
     */
    @Test
    @Order(3)
    public void testOffsetSlowerThanIndex() {
        assertNotEquals(0, indexPaginationDuration);
        assertNotEquals(0, offsetPaginationDuration);
        assertTrue(indexPaginationDuration < offsetPaginationDuration);
    }

    /**
     * Mesmo com a pagina????o via ??ndice executando dentro de uma transa????o, sofremos com o comportamento do entityManager
     * armazenar todas as entidades em mem??ria e potencialmente sofrer uma falta de recurso.
     */
    @Test
    public void testSelect_usingIndexPagination_withoutClear() {
        List<Integer> managedEntities = largeReadService.selectIndexPaginationWithTransaction(false, PAGINATION_SIZE);
        assertEquals(PAGINATION_SIZE, managedEntities.get(0));
        assertEquals(TOTAL_SIZE, lastOf(managedEntities));
    }

    /**
     * Caso seja necess??rio executar a pagina????o dentro de uma transa????o, deve-se chamar periodicamente entityManager.clear()
     * para liberar mem??ria usada pelo entityManager.
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
