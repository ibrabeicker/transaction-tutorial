package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.exception.SalaryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
public class TransactionService {

    @Autowired
    private CompanyService companyService;

    @Autowired
    private EmployeeService employeeService;

    public void createNoTransaction(Company company, List<Employee> employees) {
        company = companyService.save(company);
        for (Employee employee : employees) {
            employee.setCompany(company);
            employeeService.save(employee);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void createWithTransaction(Company company, List<Employee> employees) {
        company = companyService.save(company);
        for (Employee employee : employees) {
            employee.setCompany(company);
            employeeService.save(employee);
        }
    }

    @Transactional
    public void validateWithTransaction(Company company, List<Employee> employees) throws SalaryException {
        company = companyService.save(company);
        for (Employee employee : employees) {
            employee.setCompany(company);
            employeeService.validateAndSave(employee);
        }
    }

    @Transactional(rollbackFor = SalaryException.class)
    public void validateWithTransactionWithRollback(Company company, List<Employee> employees) throws SalaryException {
        company = companyService.save(company);
        for (Employee employee : employees) {
            employee.setCompany(company);
            employeeService.validateAndSave(employee);
        }
    }

    @Transactional
    public void saveCompanies(Company company1, Company company2) {
        companyService.saveOnNewTransaction(company1);
        log.info("Persisted {} on new transaction", company1);
        companyService.save(company2);
        log.info("Persisted {}", company2);
    }

    @Transactional(timeout = 1)
    public void saveCompaniesWithDeadlock(Company company1, Company company2) {
        companyService.save(company1);
        log.info("Persisted {}", company1);
        companyService.saveOnNewTransaction(company2);
        log.info("Persisted {} on new transaction", company2);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void saveCompaniesTransactionRequired(Company company1, Company company2) {
        companyService.save(company1);
        log.info("Persisted {}", company1);
        companyService.saveOnTransactionRequired(company2);
        log.info("Persisted {}", company2);
    }

    public void publicMethodCallingPrivateTransaction(Company company, List<Employee> employees) {
        createWithTransaction(company, employees);
    }

    @Transactional
    public void transactional(List<Runnable> runnables) {
        for (Runnable runnable : runnables) {
            runnable.run();
        }
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public void readUncommitted(List<Runnable> runnables) {
        for (Runnable runnable : runnables) {
            runnable.run();
        }
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public void repeatableRead(List<Runnable> runnables) {
        for (Runnable runnable : runnables) {
            runnable.run();
        }
    }

    @Transactional
    public void runInTransaction(Runnable runnable) {
        runnable.run();
    }

    public void runNoTransaction(Runnable runnable) {
        runnable.run();
    }
}
