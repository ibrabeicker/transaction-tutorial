package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.exception.SalaryException;
import com.pensarcomodev.transactional.repository.EmployeeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class EmployeeService {

    @Autowired
    private EmployeeRepository employeeRepository;

    public Employee save(Employee employee) {
        return employeeRepository.save(employee);
    }

    public Employee validateAndSave(Employee employee) throws SalaryException {
        if (employee.getSalary() != null && employee.getSalary().compareTo(BigDecimal.ZERO) < 0) {
            throw new SalaryException();
        }
        return save(employee);
    }

    public List<Employee> findByCompany(Company company) {
        return employeeRepository.findByCompany(company);
    }

    public List<String> deleteAll(Company company) {
        List<String> firedDocuments = new ArrayList<>();
        List<Employee> employees = findByCompany(company);
        employees.stream().map(Employee::getDocument).forEach(firedDocuments::add);
        deleteAll(employees);
        return firedDocuments;
    }

    public void deleteAll(List<Employee> employees) {
        log.info("Deleting employees {}", employees);
        employeeRepository.deleteAll(employees);
    }

    public void deleteAllBatch(List<Employee> employees) {
        log.info("Deleting employees {}", employees);
        employeeRepository.deleteAllInBatch(employees);
    }
}
