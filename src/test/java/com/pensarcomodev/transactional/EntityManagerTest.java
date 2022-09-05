package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.concurrency.ParallelTransactions;
import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.CompanyNoIdGeneration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
public class EntityManagerTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(EntityManagerTest.class);
    private Long companyId;

    @BeforeEach
    protected void setUp() {
        super.setUp();
        company = companyRepository.save(company);
        companyId = company.getId();
        log.info("Created Company id={}", companyId);
    }

    /**
     * Dentro de uma mesma transação, chamadas consecutivas que devem retornar a mesma entidade acabam por retornar a
     * mesma referência do objeto.
     */
    @Test
    public void findByIdSameTransaction_isSame() {

        AtomicReference<Company> companyBd = new AtomicReference<>();
        AtomicReference<Company> companyBd2 = new AtomicReference<>();

        transactionService.runInTransaction(() -> {
            companyBd.set(companyRepository.findById(companyId).orElseThrow());
            companyBd2.set(companyRepository.findById(companyId).orElseThrow());
        });

        assertSame(companyBd.get(), companyBd2.get());
    }

    /**
     * Quando chamado em transações diferentes, chamadas que devem retornar a mesma entidade retornam dois objetos
     * que representam a mesma entidade.
     */
    @Test
    public void findByIdWithTransaction_isNotSame() {

        AtomicReference<Company> companyBd = new AtomicReference<>();
        AtomicReference<Company> companyBd2 = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    companyBd2.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action1(() -> {})
                .execute(transactionService::transactional, transactionService::transactional);

        assertNotSame(companyBd.get(), companyBd2.get());
    }

    /**
     * Alterações feitas em outra transação não são refletidas automaticamente na entidade referenciada na transação.
     *
     * Cenário:
     * T1 faz o select de uma entidade
     * T2 faz o select da mesma entidade, altera o nome e salva
     * T1 ainda tem uma entidade com o nome anterior à alteração da T2
     */
    @Test
    public void updatePropertyOtherTransaction_nameDoesNotChange() {

        AtomicReference<Company> companyBd = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    Company company2 = companyRepository.findById(companyId).orElseThrow();
                    company2.setName("ENTERPRISE");
                    companyRepository.saveAndFlush(company2);
                })
                .action1(() -> {
                    assertEquals("COMPANY 1", companyBd.get().getName());
                })
                .execute(transactionService::transactional, transactionService::transactional);
    }

    /**
     * Mesmo cenário da anterior, porém chamando entityManager.refresh() na entidade dT1.
     *
     * Cenário:
     * T1 faz o select de uma entidade
     * T2 faz o select da mesma entidade, altera o nome e salva
     * T1 chama entityManager.refresh() e obtém o nome atualizado persistido no banco pela T2
     */
    @Test
    public void updatePropertyOtherTransactionWithRefresh_nameChange() {

        AtomicReference<Company> companyBd = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    Company company2 = companyRepository.findById(companyId).orElseThrow();
                    company2.setName("ENTERPRISE");
                    companyRepository.save(company2);
                })
                .action1(() -> {
                    entityManager.refresh(companyBd.get());
                })
                .execute(transactionService::transactional, transactionService::transactional);
        assertEquals("ENTERPRISE", companyBd.get().getName());
    }

    /**
     * A persistência do JPA transforma o save em um comando de update que atualiza todos os campos de uma entidade com
     * a condição do identificador ser igual ao identificador da entidade passada. O efeito disso é que se a intenção é
     * atualizar apenas uma propriedade e outra transação está manipulando a mesma entidade para alterar outra propriedade,
     * as alterações feitas pela T2 serão perdidas e sobrescritas pelo estado como a T1 estava no momento do save.
     *
     * Cenário:
     * T1 obtém a referência a uma entidade
     * T2 obtém a referência a mesma entidade, altera o nome e salva
     * T1 altera o documento e salva. Nesse momento a entidade de T1 ainda tem em memória o nome anterior à alteração de T2,
     * portanto o efeito de salvar nesse momento é que o JPA cria e executa um comando tal qual:
     *      update company set document='000000000000', name='COMPANY 1' where id=1
     * desfazendo a alteração anterior no nome.
     */
    @Test
    public void twoParallelUpdates_secondOverwritesFirst() {

        AtomicReference<Company> companyBd = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    Company company2 = companyRepository.findById(companyId).orElseThrow();
                    company2.setName("ENTERPRISE");
                    companyRepository.saveAndFlush(company2);
                })
                .action1(() -> {
                    Company company = companyBd.get();
                    company.setDocument("000000000000");
                    companyRepository.saveAndFlush(company);
                })
                .execute(transactionService::transactional, transactionService::transactional);

        Company company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("000000000000", company.getDocument());
        assertEquals("COMPANY 1", company.getName());
    }

    /**
     * Mesmo cenário da anterior, porém chamando entityManager.refresh() antes de cada alteração.
     * A chamada ao refresh não previne totalmente o comportamento indesejado, pois as alterações podem ocorrer entre
     * as chamadas do refresh e do save, a garantia só é obtida com um mecanismo de lock no banco.
     */
    @Test
    public void testTwoUpdate_callsRefresh_secondOverwritesFirst() {

        AtomicReference<Company> companyBd = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    Company company2 = companyRepository.findById(companyId).orElseThrow();
                    entityManager.refresh(company2);
                    company2.setName("ENTERPRISE");
                    companyRepository.saveAndFlush(company2);
                })
                .action1(() -> {
                    Company company = companyBd.get();
                    entityManager.refresh(company);
                    company.setDocument("000000000000");
                    companyRepository.saveAndFlush(company);
                })
                .execute(transactionService::transactional, transactionService::transactional);

        Company company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("000000000000", company.getDocument());
        assertEquals("ENTERPRISE", company.getName());
    }

    /**
     * Dentro de uma transação as alterações feitas após a chamada a save são persistidas da mesma forma quando a transação
     * termina. Usar o save dentro da transação é opcional.
     */
    @Test
    public void propertySetAfterSaveOnTransaction_doesPersistChange() {

        transactionService.runInTransaction(() -> {
            Company company = companyRepository.findById(companyId).orElseThrow();
            company.setName("ENTERPRISE");
            companyRepository.save(company); // opcional
            company.setName("FOO");
        });

        Company company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("FOO", company.getName());
    }

    /**
     * Fora de uma transação as alterações feitas a uma entidade lida não são persistidas
     */
    @Test
    public void propertySetAfterSaveNoTransaction_doesNotPersistChange() {

        transactionService.runNoTransaction(() -> {
            Company company = companyRepository.findById(companyId).orElseThrow();
            company.setName("ENTERPRISE");
            companyRepository.save(company);
            company.setName("FOO");
        });

        company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("ENTERPRISE", company.getName());
    }

    /**
     * Alterações feitas após a chamada a entityManager.detach() não são persistidas.
     */
    @Test
    public void propertySetAfterSaveAndDetach_doesNotPersistChange() {

        transactionService.runInTransaction(() -> {
            Company company = companyRepository.findById(companyId).orElseThrow();
            company.setName("ENTERPRISE");
            companyRepository.saveAndFlush(company);
            entityManager.detach(company);
            company.setName("FOO");
        });

        Company company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("ENTERPRISE", company.getName());
    }

    /**
     * A chamada ao save atualizando não necessariamente altera o registro no banco, é garantido que apenas no final de uma transação
     * a entidade gerenciada seja persistida. Caso seja chamado entityManager.detach() o objeto não é mais uma entidade
     * gerenciada e portanto a alteração não é persistida.
     */
    @Test
    public void updatePropertyOnTransaction_onlyUpdatesOnCommit() {

        transactionService.runInTransaction(() -> {
            Company company = companyRepository.findById(companyId).orElseThrow();
            company.setName("ENTERPRISE");
            companyRepository.save(company);
            entityManager.detach(company);
        });

        Company company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("COMPANY 1", company.getName());
    }

    /**
     * A chamada ao saveAndFlush garante que a alteração seja feita no banco no momento da chamada.
     */
    @Test
    public void saveAndFlushOnTransaction_persistsOnDatabaseBeforeCommit() {

        transactionService.runInTransaction(() -> {
            Company company = companyRepository.findById(companyId).orElseThrow();
            company.setName("ENTERPRISE");
            companyRepository.saveAndFlush(company);
            entityManager.detach(company);
        });

        Company company = companyRepository.findById(companyId).orElseThrow();
        assertEquals("ENTERPRISE", company.getName());
    }

    /**
     * A inserção de registros novos (id gerado pelo banco) acontece imediatamente, não no final da transação.
     */
    @Test
    public void insertingWithoutIdInTransaction_callsDbAndIdIsNotNull() {

        AtomicReference<Long> companyId = new AtomicReference<>();
        transactionService.runInTransaction(() -> {
            Company company = Company.builder().document("9999999999999").build();
            companyRepository.save(company);
            assertNotNull(company.getId());
            companyId.set(company.getId());
            entityManager.detach(company);
        });

        assertEquals(1, companyRepository.count(companyId.get()));
    }

    /**
     * A inserção de registros com id já definido acontece apenas no final da transação chamando save.
     */
    @Test
    public void insertingWithIdInTransaction_persistOnlyHappendOnCommit() {

        transactionService.runInTransaction(() -> {
            CompanyNoIdGeneration company = CompanyNoIdGeneration.builder().id(9999L).document("9999999999999").build();
            company = companyNoIdGenerationRepository.save(company);
            entityManager.detach(company);
        });

        assertEquals(0, companyNoIdGenerationRepository.count(9999L));
    }

    /**
     * Se uma entidade é deletada enquanto outra transação manipula a mesma, uma exceção é lançada.
     */
    @Test
    public void deleteInAnotherTransaction_causesException() {

        AtomicReference<Company> companyBd = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    companyRepository.delete(company);
                })
                .action1(() -> {
                    Company company = companyBd.get();
                    company.setName("ENTERPRISE");
                    assertThrows(ObjectOptimisticLockingFailureException.class, () -> companyRepository.saveAndFlush(company));
                })
                .execute(transactionService::transactional, transactionService::transactional);
    }

    /**
     * Se uma entidade é deletada enquanto outra transação manipula a mesma e o detach é chamado, para o JPA isso é interpretado
     * como a inserção de uma nova entidade.
     */
    @Test
    public void deleteInAnotherTransactionAndCallDetach_createsAnotherEntity() {

        AtomicReference<Company> companyBd = new AtomicReference<>();
        AtomicReference<Long> otherId = new AtomicReference<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyBd.set(companyRepository.findById(companyId).orElseThrow());
                })
                .action2(() -> {
                    companyRepository.delete(company);
                })
                .action1(() -> {
                    Company company = companyBd.get();
                    company.setName("ENTERPRISE");
                    entityManager.detach(company);
                    company = companyRepository.saveAndFlush(company);
                    otherId.set(company.getId());
                })
                .execute(transactionService::transactional, transactionService::transactional);

        assertNotEquals(otherId.get(), companyId);
    }
}
