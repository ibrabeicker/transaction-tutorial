package com.pensarcomodev.transactional;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.repository.EmployeeRepository;
import com.pensarcomodev.transactional.service.CompanyService;
import com.pensarcomodev.transactional.service.EmployeeService;
import com.pensarcomodev.transactional.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.EntityManager;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(TransactionTest.class);

    @Autowired
    protected TransactionService transactionService;

    @Autowired
    protected CompanyRepository companyRepository;

    @Autowired
    protected CompanyService companyService;

    @Autowired
    protected EmployeeRepository employeeRepository;

    @Autowired
    protected EmployeeService employeeService;

    @Autowired
    protected EntityManager entityManager;

    protected static final String COMPANY_DOCUMENT = "123456000100";
    protected static final String COMPANY_DOCUMENT_2 = "123456000101";
    protected Company company;

    @BeforeEach
    protected void setUp() {
        employeeRepository.deleteAll();
        companyRepository.deleteAll();
        company = Company.builder()
                .document(COMPANY_DOCUMENT)
                .name("COMPANY 1")
                .build();;
    }

    protected Company buildCompany() {
        return Company.builder()
                .document(COMPANY_DOCUMENT)
                .build();
    }

    protected List<Employee> buildEmployees(int num) {
        return IntStream.rangeClosed(1, num)
                .mapToObj(this::buildEmployee)
                .collect(Collectors.toList());
    }

    protected Employee buildEmployee(int num) {
        return Employee.builder()
                .document(String.format("%011d", num))
                .salary(BigDecimal.valueOf(5000))
                .build();
    }

    protected Employee buildInvalidEmployee() {
        return Employee.builder()
                .salary(BigDecimal.valueOf(5000))
                .build();
    }

    protected List<Employee> persistEmployees(int num) {
        return IntStream.rangeClosed(1, num)
                .mapToObj(i -> Employee.builder()
                        .document(String.format("%011d", i))
                        .company(company)
                        .salary(BigDecimal.valueOf(5000))
                        .build())
                .map(i -> employeeRepository.save(i))
                .collect(Collectors.toList());
    }

}
