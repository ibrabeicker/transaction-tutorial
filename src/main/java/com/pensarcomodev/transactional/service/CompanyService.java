package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
public class CompanyService {

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private EmployeeService employeeService;

    @Autowired
    private EntityManager entityManager;

    public Company save(Company entity) {
        return companyRepository.save(entity);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, timeout = 3)
    public Company saveOnNewTransaction(Company company) {
        return save(company);
    }

    @Transactional(propagation = Propagation.REQUIRED, timeout = 3)
    public Company saveOnTransactionRequired(Company company) {
        return save(company);
    }

    @Transactional
    @SneakyThrows
    public void fireEveryone(Long companyId, CountDownLatch countDownLatch, List<String> firedDocuments) {
        countDownLatch.await();
        Company company = companyRepository.findById(companyId).orElseThrow();
        List<Employee> employees = employeeService.findByCompany(company);
        employees.stream().map(Employee::getDocument).forEach(firedDocuments::add);
        employeeService.deleteAll(employees);
    }

    @Transactional
    @SneakyThrows
    public void fireEveryoneBatch(Long companyId, CountDownLatch countDownLatch, List<String> firedDocuments) {
        countDownLatch.await();
        Company company = companyRepository.findById(companyId).orElseThrow();
        List<Employee> employees = employeeService.findByCompany(company);
        employees.stream().map(Employee::getDocument).forEach(firedDocuments::add);
        employeeService.deleteAllBatch(employees);
    }

    @Transactional
    @SneakyThrows
    public void fireEveryonePessimisticWrite(Long companyId, CountDownLatch countDownLatch, List<String> firedDocuments) {
        countDownLatch.await();
        Company company = companyRepository.findByIdPessimisticWrite(companyId);
        List<Employee> employees = employeeService.findByCompany(company);
        employees.stream().map(Employee::getDocument).forEach(firedDocuments::add);
        employeeService.deleteAll(employees);
    }

    @Transactional
    @SneakyThrows
    public void fireEveryonePessimisticRead(Long companyId, CountDownLatch countDownLatch, List<String> firedDocuments) {
        countDownLatch.await();
        Company company = companyRepository.findByIdPessimisticRead(companyId);
        List<Employee> employees = employeeService.findByCompany(company);
        employees.stream().map(Employee::getDocument).forEach(firedDocuments::add);
        employeeService.deleteAll(employees);
    }

    public Company saveAndFlush(Company company) {
        company = companyRepository.saveAndFlush(company);
        log.info("Saved {}", company);
        return company;
    }

}
