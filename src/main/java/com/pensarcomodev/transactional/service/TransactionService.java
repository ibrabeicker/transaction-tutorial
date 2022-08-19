package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

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

    public void publicMethodCallingPrivateTransaction(Company company, List<Employee> employees) {
        createWithTransaction(company, employees);
    }

    private void errorMethod() {
        throw new RuntimeException("Error");
    }
}
