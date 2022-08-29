package com.pensarcomodev.transactional.repository;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.entity.Employee;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {

    @Query(value = "select * from information_schema.tables where table_name = 'employee'", nativeQuery = true)
    List<Map<String, Object>> listTables();

    @Modifying
    @Query(value = "update Employee set salary = salary + :increase where id = :id")
    void increaseSalary(@Param("id") Long id, @Param("increase") BigDecimal increase);

    @Query(value = "select e from Employee e where company = :company")
    List<Employee> findByCompany(@Param("company") Company company);
}
