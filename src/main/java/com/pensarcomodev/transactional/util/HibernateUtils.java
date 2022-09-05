package com.pensarcomodev.transactional.util;

import org.hibernate.Session;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.stat.SessionStatistics;
import org.hibernate.stat.Statistics;

import javax.persistence.EntityManager;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HibernateUtils {

    public static List<EntityEntry> getManagedEntities(Session session) {
        SessionImplementor unwrap = session.unwrap(SessionImplementor.class);
        Map.Entry<Object, EntityEntry>[] entries = unwrap.getPersistenceContext().reentrantSafeEntityEntries();
        return Arrays.stream(entries).map(e -> e.getValue()).collect(Collectors.toList());
    }

    public static int getNumberOfManagedEntities(Session session) {
        SessionImplementor unwrap = session.unwrap(SessionImplementor.class);
        return unwrap.getPersistenceContext().getNumberOfManagedEntities();
    }

    public static int getNumberOfManagedEntities2(Session session) {
        return session.getStatistics().getEntityCount();
    }

    public static SessionStatistics getSessionStatistics(EntityManager entityManager) {
        Session session = entityManager.unwrap(Session.class);
        return session.getStatistics();
    }

    public static Statistics getStatistics(EntityManager entityManager) {
        Session session = entityManager.unwrap(Session.class);
        return session.getSessionFactory().getStatistics();
    }
}
