package com.pensarcomodev.transactional.repository;

import com.pensarcomodev.transactional.entity.Company;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.LockModeType;
import java.util.List;
import java.util.stream.Stream;

@Repository
public interface CompanyRepository extends JpaRepository<Company, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from Company c where id = :id")
    Company findByIdPessimisticWrite(@Param("id") Long id);

    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query("select c from Company c where id = :id")
    Company findByIdPessimisticRead(@Param("id") Long id);

    @Query("select c from Company c")
    Stream<Company> findAllWithStream();

    @Transactional
    default List<Company> saveAllTransaction(List<Company> companyList) {
        return saveAll(companyList);
    }

    @Query("select c from Company c order by id")
    List<Company> findPaginationByIndex(Pageable limitPage);

    @Query("select c from Company c where c.id > :id order by id")
    List<Company> findPaginationByIndex(@Param("id") Long lastId, Pageable limitPage);

    @Query(value = "select count(*) from Company c where id = :id")
    int count(@Param("id") Long id);

}
