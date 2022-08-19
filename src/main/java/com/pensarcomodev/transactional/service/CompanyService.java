package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CompanyService {

    @Autowired
    private CompanyRepository companyRepository;

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
}
