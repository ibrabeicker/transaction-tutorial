package com.pensarcomodev.transactional.repository;

import com.pensarcomodev.transactional.entity.CompanyNoIdGeneration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface CompanyNoIdGenerationRepository extends JpaRepository<CompanyNoIdGeneration, Long> {

    @Query(value = "select count(*) from CompanyNoIdGeneration c where id = :id")
    int count(@Param("id") Long id);

    @Query(value = "select count(*) from company where id = :id", nativeQuery = true)
    int countNative(@Param("id") Long id);
}
