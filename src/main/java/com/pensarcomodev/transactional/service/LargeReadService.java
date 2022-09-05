package com.pensarcomodev.transactional.service;

import com.pensarcomodev.transactional.entity.Company;
import com.pensarcomodev.transactional.repository.CompanyRepository;
import com.pensarcomodev.transactional.util.HibernateUtils;
import com.pensarcomodev.transactional.util.TimeMetric;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.hibernate.stat.Statistics;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class LargeReadService {

    private final EntityManager entityManager;
    private Session session;
    private Statistics statistics;
    private final CompanyRepository companyRepository;

    @PostConstruct
    public void setup() {
        session = entityManager.unwrap(Session.class);
    }

    public int selectAllWithoutTransactionWithTransaction() {
        List<Company> all = companyRepository.findAll();
        return HibernateUtils.getNumberOfManagedEntities2(session);
    }

    @Transactional
    public int selectAllOnTransaction() {
        List<Company> all = companyRepository.findAll();
        return HibernateUtils.getNumberOfManagedEntities2(session);
    }

    public List<Integer> selectAllWithStreamWithoutTransaction() {
        return selectAllWithStream();
    }

    @Transactional
    public List<Integer> selectAllWithStreamWithTransaction() {
        return selectAllWithStream();
    }

    public List<Integer> selectAllWithStream() {
        List<Integer> entityCount = new ArrayList<>();
        Stream<Company> stream = companyRepository.findAllWithStream();
        stream.forEach(i -> {
            if (i.getId() % 100 == 0) {
                entityCount.add(HibernateUtils.getNumberOfManagedEntities2(session));
            }
        });
        return entityCount;
    }

    @Transactional
    public List<Integer> selectIndexPaginationWithTransaction(boolean callClear, int pageSize) {
        return selectIndexPaginationWithoutTransaction(callClear, pageSize);
    }

    public List<Integer> selectIndexPaginationWithoutTransaction(boolean callClear, int pageSize) {
        List<Integer> entityCount = new ArrayList<>();
        Pageable page = PageRequest.of(0, pageSize);
        Long lastId;
        List<Company> paginationByIndex = companyRepository.findPaginationByIndex(page);
        while (!paginationByIndex.isEmpty()) {
            lastId = paginationByIndex.get(paginationByIndex.size() - 1).getId();
            entityCount.add(HibernateUtils.getNumberOfManagedEntities2(session));
            if (callClear) {
                entityManager.clear();
            }
            paginationByIndex = companyRepository.findPaginationByIndex(lastId, page);
        }
        return entityCount;
    }

    public List<Integer> selectInBatchesWithOffsetPagination(int pageSize) {
        List<Integer> queryTime = new ArrayList<>();
        Pageable page = PageRequest.of(0, pageSize);
        List<Company> paginationByIndex = companyRepository.findPaginationByIndex(page);
        while (!paginationByIndex.isEmpty()) {
            TimeMetric timeMetric = new TimeMetric();
            paginationByIndex = companyRepository.findPaginationByIndex(page);
            queryTime.add((int) timeMetric.getDuration());
            page = page.next();
            log.info("page {}", page);
        }
        return queryTime;
    }

}
