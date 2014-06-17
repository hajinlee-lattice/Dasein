package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Repository
public abstract class BaseDaoImpl<T extends HasPid> implements BaseDao<T> {

    /**
     * Class presentation of the entity object that the subclass Dao is working
     * with.
     */
    protected abstract Class<T> getEntityClass();

    @Autowired
    protected SessionFactory sessionFactory;

    BaseDaoImpl() {
    }

    /**
     * This is a generic create for the ORM layer. This should work for all
     * entity types.
     * 
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(T entity) {
        sessionFactory.getCurrentSession().persist(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public boolean containInSession(T entity) {
        boolean bContains = sessionFactory.getCurrentSession().contains(entity);

        return bContains;
    }

    /**
     * Either create(Object) or update(Object) the given instance, depending
     * upon resolution of the unsaved-value checks (see the manual for
     * discussion of unsaved-value checking). This operation cascades to
     * associated instances if the association is mapped with
     * cascade="save-update"
     * 
     * 
     * @param entity
     *            - Parameters: object - a transient or detached instance
     *            containing new or updated state
     * 
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(T entity) {
        sessionFactory.getCurrentSession().saveOrUpdate(entity);
    }

    /**
     * Find an entity by key
     * 
     * @param entity
     *            - entity.pid must NOT be null.
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly=true)
    public T findByKey(T entity) {
        Class<?> clz = entity.getClass();

        return (T) sessionFactory.getCurrentSession().get(clz, entity.getPid());
    }

    @SuppressWarnings("unchecked")
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly=true)
    public T findByKey(Class<T> entityClz, Long key) {
        return (T) sessionFactory.getCurrentSession().get(entityClz, key);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly=true)
    public List<T> findAll() {
        Session session = sessionFactory.getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query query = session.createQuery("from " + entityClz.getSimpleName());
        return query.list();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void update(T entity) {
        sessionFactory.getCurrentSession().update(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(T entity) {
        sessionFactory.getCurrentSession().delete(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteAll() {
        Session session = sessionFactory.getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query query = session.createQuery("delete from " + entityClz.getSimpleName());
        query.executeUpdate();
    }
}
