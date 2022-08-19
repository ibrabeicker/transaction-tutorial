package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.exception.SalaryException;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.repository.EmployeeRepository;
import com.pensarcomodev.transactional.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

//@DataJpaTest
@Testcontainers
@ActiveProfiles("test")
//@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = NOT_SUPPORTED) // we're going to handle transactions manually
@SpringBootTest
public class TransactionTest {

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private EmployeeRepository employeeRepository;

    private static final String COMPANY_DOCUMENT = "123456000100";
    private static final String COMPANY_DOCUMENT_2 = "123456000101";

    @BeforeEach
    private void setUp() {
        employeeRepository.deleteAll();
        companyRepository.deleteAll();
    }

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
        Company company1 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 1")
                .build();
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT_2)
                .name("COMPANY 2")
                .build();

        assertThrows(QueryTimeoutException.class, () -> transactionService.saveCompaniesWithDeadlock(company1, company2));

        assertEquals(0, companyRepository.count());
    }

    /**
     * A propagação padrão da @Transactional é REQUIRED, o que significa que se não há uma transação corrente, ela é criada,
     * caso já exista uma, ela participará da transação corrente. A consequência disso é que caso o método transacional interno
     * lance algum erro, toda a transação é abortada
     */
    @Test
    public void testTransactionRequired_dataIntegrityRollbackEverything() {
        Company company1 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 1")
                .build();
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 2")
                .build();

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.saveCompaniesTransactionRequired(company1, company2));

        assertEquals(0, companyRepository.count());
    }

    /**
     * Para que o erro de operações posteriores não desfaçam as anteriores, é necessário separar as transações, abrindo
     * e concluindo a primeira antes de chamar a segunda
     */
    @Test
    public void testSequentialTransactions_dataIntegrityRollbackSecond() {
        Company company1 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 1")
                .build();
        Company company2 = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 2")
                .build();

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.saveCompaniesSequentialTransactions(company1, company2));

        assertEquals(1, companyRepository.count());
        assertEquals("COMPANY 1", companyRepository.findAll().get(0).getName());
    }

    @Test
    public void


    private Company buildCompany() {
        return Company.builder()
                .document(COMPANY_DOCUMENT)
                .build();
    }

    private List<Employee> buildEmployees(int num) {
        return IntStream.rangeClosed(1, num + 1)
                .mapToObj(this::buildEmployee)
                .collect(Collectors.toList());
    }

    private Employee buildEmployee(int num) {
        return Employee.builder()
                .document(String.format("%011d", num))
                .salary(BigDecimal.valueOf(5000))
                .build();
    }

    private Employee buildInvalidEmployee() {
        return Employee.builder()
                .salary(BigDecimal.valueOf(5000))
                .build();
    }

}
