package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.concurrency.PingPongLock;
import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.exception.SalaryException;
import com.pensarcomodev.transactional.repository.EmployeeRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
public class EmployeeService {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    private EntityManager entityManager;

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

    @Transactional
    @SneakyThrows
    public void increaseSalary(Long employeeId, BigDecimal increase, PingPongLock lock) {
        log.info("Before increaseSalary");
        employeeRepository.increaseSalary(employeeId, increase);
        log.info("After increaseSalary");
        lock.end();
        Thread.sleep(200);
    }

    @Transactional
    public void increaseSalary2(Long employeeId, BigDecimal increase, PingPongLock lock) {
        lock.waitPing();
        log.info("Before increaseSalary");
        employeeRepository.increaseSalary(employeeId, increase);
        log.info("After increaseSalary");
        lock.end();
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
