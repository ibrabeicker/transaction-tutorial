package com.pensarcomodev.transactional.repository;

import com.pensarcomodev.transactional.entity.Employee;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {

    @Query(value = "select * from information_schema.tables where table_name = 'employee'", nativeQuery = true)
    List<Map<String, Object>> listTables();
}
