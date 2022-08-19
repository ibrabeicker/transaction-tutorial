package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.repository.EmployeeRepository;
import com.pensarcomodev.transactional.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
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

    @BeforeEach
    private void setUp() {
        List<Map<String, Object>> objects = employeeRepository.listTables();
        System.out.println(objects);
        employeeRepository.deleteAll();
        companyRepository.deleteAll();
    }

    @Test
    public void testWithTransaction_errorMustRollBack() {

        List<Employee> employees = Arrays.asList(buildEmployee(1), buildInvalidEmployee(), buildEmployee(2));

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.createWithTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count());
        assertEquals(0, employeeRepository.count());
    }

    @Test
    public void testWithoutTransaction_errorDoesntRollBack() {

        List<Employee> employees = Arrays.asList(buildEmployee(1), buildInvalidEmployee(), buildEmployee(2));

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.createNoTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count());
        assertEquals(0, employeeRepository.count());
    }

    @Test
    public void testPrivateMethodWithTransaction_errorDoesntRollBack() {

        List<Employee> employees = Arrays.asList(buildEmployee(1), buildInvalidEmployee(), buildEmployee(2));

        assertThrows(DataIntegrityViolationException.class, () -> transactionService.publicMethodCallingPrivateTransaction(buildCompany(), employees));

        assertEquals(0, companyRepository.count(), "Expected no companies persisted");
        assertEquals(0, employeeRepository.count(), "Expected no employee persisted");
    }

    private Company buildCompany() {
        return Company.builder()
                .document("123456000100")
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
