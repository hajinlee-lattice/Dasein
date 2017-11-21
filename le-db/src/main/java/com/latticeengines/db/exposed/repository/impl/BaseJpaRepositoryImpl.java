package com.latticeengines.db.exposed.repository.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.repository.support.JpaEntityInformation;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.orm.jpa.vendor.HibernateJpaSessionFactoryBean;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;

/**
 * Extends the Spring SimpleJpaRepository. Allows us to override any of the base classes if needed.
 *
 * @param <T>
 * @param <ID>
 */
public class BaseJpaRepositoryImpl<T, ID extends Serializable> extends SimpleJpaRepository<T, ID> implements BaseJpaRepository<T,ID>{

    private static final Logger log = LoggerFactory.getLogger(BaseJpaRepositoryImpl.class);

    private final JpaEntityInformation<T, ?> entityInformation;
	private final EntityManager entityManager;
	
	// We still need to use sessionFactory for all DML operations.
	// 	Because, we are still using HibernateTransactionManager.
	//	If we want to use, EntityManager for DML, then we need to switch to JPATransactionManager in our persistance configurations
	private final SessionFactory sessionFactory;
	
	public BaseJpaRepositoryImpl(JpaEntityInformation<T, ?> entityInformation, EntityManager entityManager) {
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

	/*
	@SuppressWarnings("unchecked")
	@Override
	public <S extends T> S save(S entity) {
		if (entityInformation.isNew(entity)) {
			getSessionFactory().getCurrentSession().persist(entity);
			return entity;
		} else {
			return (S) getSessionFactory().getCurrentSession().merge(entity);
		}
	}
	
	@Override
	public void delete(T entity) {
		if(log.isDebugEnabled())	
			log.debug("Deleting Entity: " + entity);
		if (entity == null) {
			return;
		}
		
		Session currSession = getSessionFactory().getCurrentSession();
		currSession.delete(currSession.contains(entity) ? entity: currSession.merge(entity));
		currSession.flush();
	}

	@Override
	public void flush() {
		getSessionFactory().getCurrentSession().flush();
	}
	*/
}
