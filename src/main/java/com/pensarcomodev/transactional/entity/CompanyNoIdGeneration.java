package com.pensarcomodev.transactional.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@Entity
@Table(name = "company")
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Cacheable(false)
public class CompanyNoIdGeneration {

    @Id
    private Long id;

    private String document;

    private String name;
}
