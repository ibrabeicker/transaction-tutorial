package com.pensarcomodev.transactional.repository;

import com.pensarcomodev.transactional.entity.CompanyNoIdGeneration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CompanyNoIdGenerationRepository extends JpaRepository<CompanyNoIdGeneration, Long> {
}
