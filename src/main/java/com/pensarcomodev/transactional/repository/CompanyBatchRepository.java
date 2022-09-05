package com.pensarcomodev.transactional.repository;

import com.pensarcomodev.transactional.entity.Company;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class CompanyBatchRepository {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public void saveAll(List<Company> companies) {
        jdbcTemplate.batchUpdate("insert into company (document) " +
                        "VALUES (?)",
                companies,
                50,
                (PreparedStatement ps, Company company) -> {
                    ps.setString(1, company.getDocument());
                });
    }
}
