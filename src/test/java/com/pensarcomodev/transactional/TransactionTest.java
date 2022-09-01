package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.concurrency.ParallelTransactions;
import com.pensarcomodev.transactional.concurrency.SequenceLock;
import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.exception.SalaryException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

@ActiveProfiles("test")
@Transactional(propagation = NOT_SUPPORTED)
@SpringBootTest
public class TransactionTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(TransactionTest.class);

    private Company company1 = Company.builder()
            .document(COMPANY_DOCUMENT)
            .name("COMPANY 1")
            .build();
    private Company company2 = Company.builder()
            .document(COMPANY_DOCUMENT_2)
            .name("COMPANY 2")
            .build();
    private Company company2Duplicated = Company.builder()
            .document(COMPANY_DOCUMENT)
            .name("COMPANY 2")
            .build();

    /**
     * Quando a anotação @Transactional está presente, ou todas as operações são bem sucedidas ou nenhuma operação é bem sucedida.
     *
     * Ao tentar persistir um registro inválido, uma exceção de violação de DataIntegrity é lançada e todas as operações
     * são revertidas e nenhum registro é persistido no banco.
     */
    @Test
    public void testWithTransaction_errorMustRollBack() {

        List<Employee> employees = Arrays.asList(buildEmployee(1), buildInvalidEmployee(), buildEmployee(2));

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.createWithTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count());
        assertEquals(0, employeeRepository.count());
    }

    /**
     * Na ausência da anotação @Transactional, cada operação no banco é finalizada na sua chamada. Na ocorrência de um
     * erro, as operações anteriores estarão persistidas e as operações posteriores não terão sido persistidas.
     */
    @Test
    public void testWithoutTransaction_errorDoesntRollBack() {

        List<Employee> employees = Arrays.asList(buildEmployee(1), buildInvalidEmployee(), buildEmployee(2));

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.createNoTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count());
        assertEquals(0, employeeRepository.count());
    }

    /**
     * A anotação @Transactional só funciona em chamadas feitas de fora da classe. Métodos privados ou métodos públicos
     * sendo chamados pela própria classe não terão efeito.
     */
    @Test
    public void testPrivateMethodWithTransaction_errorDoesntRollBack() {

        List<Employee> employees = Arrays.asList(buildEmployee(1), buildInvalidEmployee(), buildEmployee(2));

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.publicMethodCallingPrivateTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count(), "Expected no companies persisted");
        assertEquals(0, employeeRepository.count(), "Expected no employee persisted");
    }

    /**
     * A transação por padrão faz rollback apenas de exceções de runtime
     */
    @Test
    public void testCheckedException_doesntRollBack() {
        List<Employee> employees = buildEmployees(3);
        employees.get(1).setSalary(BigDecimal.valueOf(-100));

        assertThrows(SalaryException.class, () -> transactionService.validateWithTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count(), "Expected no companies persisted");
        assertEquals(0, employeeRepository.count(), "Expected no employee persisted");
    }

    /**
     * Para fazer rollback na ocorrência de exceções checadas, é necessário adicionar a propriedade rollbackFor = X.class
     */
    @Test
    public void testCheckedExceptionWithAnnotation_doesRollBack() {
        List<Employee> employees = buildEmployees(3);
        employees.get(1).setSalary(BigDecimal.valueOf(-100));

        assertThrows(SalaryException.class, () -> transactionService.validateWithTransactionWithRollback(buildCompany(), employees));

        assertEquals(0, companyRepository.count(), "Expected no companies persisted");
        assertEquals(0, employeeRepository.count(), "Expected no employee persisted");
    }

    /**
     * A persistência feita em um método com REQUIRES_NEW continua mesmo após a transação do método de fora sofrer um rollback
     */
    @Test
    public void testTwoTransactionsPersisting_firstPersistedMustSucceed() {

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.saveCompanies(company1, company2Duplicated));

        assertEquals(1, companyRepository.count());
        assertEquals("COMPANY 1", companyRepository.findAll().get(0).getName());
    }

    /**
     * Em duas transações paralelas, se ambas tentam inserir registros que não causam violação nenhuma é bloqueada
     */
    @Test
    public void testSimultaneousTransactionsPersisting_noViolationDoesntBlock() {

        ParallelTransactions.builder()
                .action1(() -> {
                    companyRepository.saveAndFlush(company1);
                    log.info("Persisted 1");
                })
                .action2(() -> {
                    companyRepository.saveAndFlush(company2);
                    log.info("Persisted 2");
                })
                .action1(() -> {
                })
                .execute(transactionService::transactional, transactionService::transactional);

        assertEquals(2, companyRepository.count());
        assertEquals("COMPANY 1", companyRepository.findAll().get(0).getName());
    }

    /**
     * Em duas transações paralelas, se ambas tentam inserir registros que causam violação, a segunda é bloqueada esperando
     * o commit ou rollback da primeira
     */
    @Test
    public void testSimultaneousTransactionsPersisting_violationDoesBlock() {

        SequenceLock sequenceLock = new SequenceLock();

        ParallelTransactions.builder()
                .action1(() -> {
                    companyRepository.saveAndFlush(company1);
                    log.info("Persisted 1");
                })
                .expectBlock2(() -> {
                    try {
                        companyRepository.saveAndFlush(company2Duplicated);
                        log.info("Persisted 2");
                    } catch (Exception e) {
                        sequenceLock.expectBlocking();
                        throw e;
                    }
                })
                .action1(() -> {
                    sequenceLock.expectRunFirst();
                })
                .execute(transactionService::transactional, i -> withException(i, DataIntegrityViolationException.class));

        assertTrue(sequenceLock.isRightOrder());
    }

    /**
     * Usar uma transação REQUIRES_NEW dentro de outra transação corrente é perigoso. Nesse cenário uma transação começa
     * na primeira chamada ao companyService.save(company1) e em seguida é chamado o companyService.saveOnNewTransaction(company2).
     * O retorno do saveOnNewTransaction ficará travado indefinidamente esperando que a primeira transação seja concluída,
     * o que será impossível pois isso só acontecerá depois do retorno do método saveOnNewTransaction, causando um deadlock.
     *
     * Deadlocks não devem ser tratados com o timeout na transação, mas sim evitados com uma implementação correta.
     */
    @Test
    public void testTwoTransactionsPersisting_causesDeadlock() {

        assertThrows(JpaSystemException.class, () -> transactionService.saveCompaniesWithDeadlock(company, company2Duplicated));

        assertEquals(0, companyRepository.count());
    }

    /**
     * A propagação padrão da @Transactional é REQUIRED, o que significa que se não há uma transação corrente, ela é criada,
     * caso já exista uma, ela participará da transação corrente. A consequência disso é que caso o método transacional interno
     * lance algum erro, toda a transação é abortada
     */
    @Test
    public void testTransactionRequired_dataIntegrityRollbackEverything() {

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.saveCompaniesTransactionRequired(company, company2Duplicated));

        assertEquals(0, companyRepository.count());
    }

    /**
     * Para que o erro de operações posteriores não desfaçam as anteriores, é necessário separar as transações, abrindo
     * e concluindo a primeira antes de chamar a segunda
     */
    @Test
    public void testSequentialTransactions_dataIntegrityRollbackSecond() {

        companyService.saveOnNewTransaction(company);
        log.info("Persisted {} on new transaction", company);
        assertThrows(DataIntegrityViolationException.class, () -> companyService.saveOnNewTransaction(company2Duplicated));

        assertEquals(1, companyRepository.count());
        assertEquals("COMPANY 1", companyRepository.findAll().get(0).getName());
    }

    /**
     * Tenta fazer o count de entidades que ainda não foram persistidas em uma transação paralela no tipo de isolamento
     * READ_UNCOMMITED.
     *
     * Comportamento esperado:
     * Transação 1 começa com o isolamento READ_UNCOMMITED
     * Transação 2 começa e persiste um registro e pausa sem comitar
     * Transação 1 faz o count dos registros persistidos e recebe 1 do registro ainda não comitado
     * Transação 2 comita
     * Transação 1 faz o count novamente e recebe a mesma contagem de 1 de antes da transação 2 comitar
     *
     * Resultado:
     * O banco Postgres não permite tal tipo de isolamento e usa seu comportamento padrão
     */
    @Test
    public void testCountWhileInserting_readUncommited() {

        List<Integer> results = new ArrayList<>();

        ParallelTransactions.builder()
                .action1(() -> {
                    int count = (int) companyRepository.count();
                    log.info("Selecting companies, result={}", count);
                    results.add(count);
                })
                .action2(() -> {
                    companyService.saveAndFlush(company);
                })
                .action1(() -> {
                    int count2 = (int) companyRepository.count();
                    log.info("Selecting companies, result={}", count2);
                    results.add(count2);
                })
                .execute(transactionService::readUncommitted, transactionService::transactional);

        assertEquals(1, results.get(0), "Through dirty reads the first select should return 1 uncommited");
        assertEquals(1, results.get(1));
    }

    /**
     * Um isolamento NON_REPEATABLE_READ significa que durante uma transação não há nenhuma garantia que múltiplas leituras
     * de um mesmo registro retorne os mesmos valores.
     *
     * Comportamento esperado:
     * Registro presente no banco
     * Transação 1 começa com o isolamento default
     * Transação 1 lê o registro com nome "COMPANY 1"
     * Transação 2 começa e altera o nome do registro para "ENTERPRISE"
     * Transação 2 comita
     * Transação 1 lê novamente o registro que dessa vez está com nome "ENTERPRISE"
     */
    @Test
    public void testNonRepeatableRead() {

        List<String> names = testRepeatableRead(transactionService::transactional);

        assertEquals("COMPANY 1", names.get(0));
        assertEquals("ENTERPRISE", names.get(1));
    }

    /**
     * Com o isolamento REPEATABLE_READ é garantido que leituras de um mesmo registro dentro de uma transação retornem
     * sempre os mesmos valores.
     *
     * Comportamento esperado:
     * Registro presente no banco
     * Transação 1 começa com o isolamento REPEATABLE_READ
     * Transação 1 lê o registro com nome "COMPANY 1"
     * Transação 2 começa e altera o nome do registro para "ENTERPRISE"
     * Transação 2 comita
     * Transação 1 lê novamente o registro que continua com o nome "COMPANY 1"
     */
    @Test
    public void testRepeatableRead() {

        List<String> names = testRepeatableRead(transactionService::repeatableRead);

        assertEquals("COMPANY 1", names.get(0));
        assertEquals("COMPANY 1", names.get(1));
    }

    public List<String> testRepeatableRead(Consumer<List<Runnable>> transactionalMethod) {

        company = companyService.save(company);
        Long companyId = company.getId();
        List<String> names = new ArrayList<>();

        ParallelTransactions parallelTransactions = ParallelTransactions.builder()
                .action1(() -> {
                    Company company = companyRepository.findById(companyId).orElseThrow();
                    String nameBeforeUpdate = company.getName();
                    log.info("Selected name {}", nameBeforeUpdate);
                    names.add(nameBeforeUpdate);
                })
                .action2(() -> {
                    Company company = companyRepository.findById(companyId).orElseThrow();
                    company.setName("ENTERPRISE");
                    companyRepository.save(company);
                    log.info("Commiting update of name ENTERPRISE");
                })
                .action1(() -> {
                    entityManager.clear(); // Necessário para evitar que a próxima query use o cache do hibernate
                    Company company = companyRepository.findById(companyId).orElseThrow();
                    String nameAfterUpdate = company.getName();
                    log.info("Selected name {}", nameAfterUpdate);
                    names.add(nameAfterUpdate);
                });
        parallelTransactions.execute(transactionalMethod, transactionService::transactional);
        return names;
    }

    /**
     * Updates paralelos na mesma entidade causam a segunda transação a esperar pelo commit ou rollback da primeira
     */
    @Test
    public void testParallelUpdate() {

        company = companyRepository.save(company);
        Employee employee = buildEmployee(1);
        employee.setCompany(company);
        employee = employeeRepository.save(employee);
        Long employeeId = employee.getId();
        SequenceLock sequenceLock = new SequenceLock();

        ParallelTransactions.builder()
                .action1(() -> {
                    employeeRepository.increaseSalary(employeeId, BigDecimal.valueOf(100));
                })
                .expectBlock2(() -> {
                    employeeRepository.increaseSalary(employeeId, BigDecimal.valueOf(300));
                    sequenceLock.expectBlocking();
                })
                .action1(() -> {
                    sequenceLock.expectRunFirst();
                })
                .execute(transactionService::transactional, transactionService::transactional);

        entityManager.clear();
        Employee employeeDb = employeeRepository.findById(employeeId).orElseThrow();
        assertEquals(5400, employeeDb.getSalary().intValue());
        assertTrue(sequenceLock.isRightOrder());
    }

    private <T extends Exception> void withException(List<Runnable> runnables, Class<T> exception) {
        assertThrows(exception, () -> transactionService.transactional(runnables));
    }

}
