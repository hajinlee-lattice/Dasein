package com.latticeengines.db.exposed.dao.impl;

import java.util.List;

import javax.sql.DataSource;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.orm.hibernate4.SessionFactoryUtils;
import org.springframework.stereotype.Repository;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Repository
public abstract class BaseDaoWithAssignedSessionFactoryImpl<T extends HasPid> implements BaseDao<T> {

    protected SessionFactory sessionFactory;

    protected SessionFactory getSessionFactory() {
        return this.sessionFactory;
    }

    /**
     * By default, all dao's are autowired with le-db's sessionFactory. However,
     * this setter is available for custom data sources to inject their own
     * custom sessionFactory from Spring XML.
     *
     * @param factory
     */
    public void setSessionFactory(SessionFactory factory) {
        this.sessionFactory = factory;
    }

    /**
     * Class presentation of the entity object that the subclass Dao is working
     * with.
     */
    protected abstract Class<T> getEntityClass();

    protected BaseDaoWithAssignedSessionFactoryImpl() {
    }

    /**
     * This is a generic create for the ORM layer. This should work for all
     * entity types.
     *
     */
    @Override
    public void create(T entity) {
        getSessionFactory().getCurrentSession().persist(entity);
    }

    @Override
    public boolean containInSession(T entity) {
        boolean bContains = getSessionFactory().getCurrentSession().contains(entity);

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
    public void createOrUpdate(T entity) {
        getSessionFactory().getCurrentSession().saveOrUpdate(entity);
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
    public T findByKey(T entity) {
        Class<?> clz = entity.getClass();

        return (T) getSessionFactory().getCurrentSession().get(clz, entity.getPid());
    }

    @SuppressWarnings("unchecked")
    @Override
    public T findByKey(Class<T> entityClz, Long key) {
        return (T) getSessionFactory().getCurrentSession().get(entityClz, key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> findAll() {
        Session session = getSessionFactory().getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query query = session.createQuery("from " + entityClz.getSimpleName());
        return query.list();
    }

    @Override
    public void update(T entity) {
        getSessionFactory().getCurrentSession().update(entity);
    }

    @Override
    public void delete(T entity) {
        getSessionFactory().getCurrentSession().delete(entity);
    }

    @Override
    public void deleteAll() {
        Session session = getSessionFactory().getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query query = session.createQuery("delete from " + entityClz.getSimpleName());
        query.executeUpdate();
    }

    protected DataSource getDataSource() {
        return SessionFactoryUtils.getDataSource(getSessionFactory());
    }
}
