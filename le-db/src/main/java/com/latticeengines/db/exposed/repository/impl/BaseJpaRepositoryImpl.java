package com.latticeengines.db.exposed.repository.impl;

import java.io.Serializable;

import javax.persistence.EntityManager;

import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.repository.support.JpaEntityInformation;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;

/**
 * Extends the Spring SimpleJpaRepository. Allows us to override any of the base
 * classes if needed.
 *
 * @param <T>
 * @param <ID>
 */
public class BaseJpaRepositoryImpl<T, ID extends Serializable> extends SimpleJpaRepository<T, ID>
        implements BaseJpaRepository<T, ID> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(BaseJpaRepositoryImpl.class);

    @SuppressWarnings("unused")
    private final JpaEntityInformation<T, ?> entityInformation;
    private final EntityManager entityManager;

    // We still need to use sessionFactory for all DML operations.
    // Because, we are still using HibernateTransactionManager.
    // If we want to use, EntityManager for DML, then we need to switch to
    // JPATransactionManager in our persistence configurations
    private final SessionFactory sessionFactory;

    public BaseJpaRepositoryImpl(JpaEntityInformation<T, ?> entityInformation,
            EntityManager entityManager) {
        super(entityInformation, entityManager);

        this.entityInformation = entityInformation;
        this.entityManager = entityManager;
        this.sessionFactory = entityManager.getEntityManagerFactory().unwrap(SessionFactory.class);
    }

    @Override
    public EntityManager getEntityManager() {
        return entityManager;
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
