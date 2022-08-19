package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Employee;
import com.pensarcomodev.transactional.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EmployeeService {

    @Autowired
    private EmployeeRepository employeeRepository;

    public Employee save(Employee employee) {
        return employeeRepository.save(employee);
    }

}
