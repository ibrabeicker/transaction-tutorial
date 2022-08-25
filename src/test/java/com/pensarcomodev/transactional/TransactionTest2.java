package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.concurrency.ParallelTransactions;
import com.pensarcomodev.transactional.concurrency.PingPongLock;
import com.pensarcomodev.transactional.concurrency.TransactionActionsRunner;
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
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

//@DataJpaTest
@Testcontainers
@ActiveProfiles("test")
//@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = NOT_SUPPORTED) // we're going to handle transactions manually
@SpringBootTest
public class TransactionTest2 extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(TransactionTest2.class);

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

    @Test
    public void testTwoTransactionsPersisting_firstPersistedMustSucceed() {
        Company company1 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 1")
                .build();
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 2")
                .build();

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.saveCompanies(company1, company2));

        assertEquals(1, companyRepository.count());
        assertEquals("COMPANY 1", companyRepository.findAll().get(0).getName());
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
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT_2)
                .name("COMPANY 2")
                .build();

        assertThrows(JpaSystemException.class, () -> transactionService.saveCompaniesWithDeadlock(company, company2));

        assertEquals(0, companyRepository.count());
    }

    /**
     * A propagação padrão da @Transactional é REQUIRED, o que significa que se não há uma transação corrente, ela é criada,
     * caso já exista uma, ela participará da transação corrente. A consequência disso é que caso o método transacional interno
     * lance algum erro, toda a transação é abortada
     */
    @Test
    public void testTransactionRequired_dataIntegrityRollbackEverything() {
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 2")
                .build();

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.saveCompaniesTransactionRequired(company, company2));

        assertEquals(0, companyRepository.count());
    }

    /**
     * Para que o erro de operações posteriores não desfaçam as anteriores, é necessário separar as transações, abrindo
     * e concluindo a primeira antes de chamar a segunda
     */
    @Test
    public void testSequentialTransactions_dataIntegrityRollbackSecond() {
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 2")
                .build();

        companyService.saveOnNewTransaction(company);
        log.info("Persisted {} on new transaction", company);
        assertThrows(DataIntegrityViolationException.class, () -> companyService.saveOnNewTransaction(company2));

        assertEquals(1, companyRepository.count());
        assertEquals("COMPANY 1", companyRepository.findAll().get(0).getName());
    }

    @Test
    public void testPingPong() {
        transactionService.pingPong();
    }

    /**
     * O count só retorna 1 após o commit da segunda transação
     */
    @Test
    public void testCountWhileInserting() {

        PingPongLock lock = new PingPongLock();
        Thread thread = new Thread(() -> companyService.saveAndWaitSemaphore(company, lock));
        thread.start();
        List<Integer> counts = companyService.countCompanies(lock);

        assertEquals(0, counts.get(0));
        assertEquals(1, counts.get(1));
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

        ParallelTransactions parallelTransactions = new ParallelTransactions(null, null)
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
                });
        parallelTransactions.execute(i -> transactionService.transactional(i), i -> transactionService.readUncommitted(i));

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

        company = companyService.save(company);

        PingPongLock lock = new PingPongLock();
        Long companyId = company.getId();
        Thread thread = new Thread(() -> companyService.update(companyId, lock));
        thread.start();
        List<String> names = companyService.selectBeforeAndAfterUpdate(companyId, lock);

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

        company = companyRepository.save(company);

        PingPongLock lock = new PingPongLock();
        Long companyId = company.getId();
        Thread thread = new Thread(() -> companyService.update(companyId, lock));
        thread.start();
        List<String> names = companyService.selectBeforeAndAfterUpdateRepeatableRead(companyId, lock);

        assertEquals("COMPANY 1", names.get(0));
        assertEquals("COMPANY 1", names.get(1));
    }

    @Test
    public void testParallelUpdates() throws InterruptedException {

        company = companyRepository.save(company);
        Employee employee = buildEmployee(1);
        employee.setCompany(company);
        employee = employeeRepository.save(employee);
        Long employeeId = employee.getId();
        PingPongLock lock = new PingPongLock();

        Thread thread1 = new Thread(() -> employeeService.increaseSalary(employeeId, BigDecimal.valueOf(100), lock));
        Thread thread2 = new Thread(() -> employeeService.increaseSalary2(employeeId, BigDecimal.valueOf(300), lock));
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        entityManager.clear();
        Employee employeeDb = employeeRepository.findById(employeeId).orElseThrow();
        assertEquals(BigDecimal.valueOf(5400), employeeDb.getSalary());
    }

}
