package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.concurrency.PingPongLock;
import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.ArrayList;
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
    public List<Integer> countCompanies(PingPongLock lock) {
        List<Integer> results = new ArrayList<>();
        lock.ping();
        int count = (int) companyRepository.count();
        log.info("Selecting companies, result={}", count);
        results.add(count);
        lock.ping();
        int count2 = (int) companyRepository.count();
        log.info("Selecting companies, result={}", count2);
        results.add(count2);
        return results;
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public List<Integer> countCompaniesUncommited(PingPongLock lock) {
        return countCompanies(lock);
    }

    @Transactional
    public void saveAndWaitSemaphore(Company company, PingPongLock lock) {
        lock.waitPing();
        companyRepository.saveAndFlush(company);
        log.info("Saved {}", company);
        lock.pong();
        log.info("Commiting transaction");
        lock.asyncEnd(200);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public List<String> selectBeforeAndAfterUpdateRepeatableRead(Long companyId, PingPongLock lock) {
        return selectBeforeAndAfterUpdate(companyId, lock);
    }

    @Transactional
    public List<String> selectBeforeAndAfterUpdate(Long companyId, PingPongLock lock) {
        List<String> names = new ArrayList<>();
        Company company = companyRepository.findById(companyId).orElseThrow();
        String nameBeforeUpdate = company.getName();
        log.info("Selected name {}", nameBeforeUpdate);
        names.add(nameBeforeUpdate);
        entityManager.detach(company); // Necessário para evitar que a próxima query use o cache do hibernate
        lock.ping();
        String nameAfterUpdate = companyRepository.findById(companyId).map(Company::getName).orElseThrow();
        log.info("Selected name {}", nameAfterUpdate);
        names.add(nameAfterUpdate);
        return names;
    }

    @Transactional
    public void update(Long companyId, PingPongLock lock) {
        lock.waitPing();
        Company company = companyRepository.findById(companyId).orElseThrow();
        company.setName("ENTERPRISE");
        companyRepository.saveAndFlush(company);
        lock.asyncEnd(400);
        log.info("Commiting transaction");
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

    @Transactional
    @SneakyThrows
    public int countEmployeesPessimisticRead(Long companyId, CountDownLatch countDownLatch) {
        Company company = companyRepository.findByIdPessimisticRead(companyId);
        countDownLatch.countDown();
        Thread.sleep(100);
        int size = employeeService.findByCompany(company).size();
        return size;
    }

    public Company saveAndFlush(Company company) {
        company = companyRepository.saveAndFlush(company);
        log.info("Saved {}", company);
        return company;
    }

}
