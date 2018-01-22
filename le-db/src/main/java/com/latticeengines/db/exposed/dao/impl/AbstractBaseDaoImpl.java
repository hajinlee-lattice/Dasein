package com.latticeengines.db.exposed.dao.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.orm.hibernate5.SessionFactoryUtils;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;

public abstract class AbstractBaseDaoImpl<T extends HasPid> implements BaseDao<T> {

    protected abstract SessionFactory getSessionFactory();

    /**
     * Class presentation of the entity object that the subclass Dao is working
     * with.
     */
    protected abstract Class<T> getEntityClass();

    /**
     * This is a generic create for the ORM layer. This should work for all
     * entity types.
     */
    @Override
    public void create(T entity) {
        setAuditingFields(entity);
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
     * @param entity
     *            - Parameters: object - a transient or detached instance
     *            containing new or updated state
     */
    @Override
    public void createOrUpdate(T entity) {
        setAuditingFields(entity);
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
    public <F> T findByField(String fieldName, F fieldValue) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where %s = :value", getEntityClass().getSimpleName(), fieldName);
        Query query = session.createQuery(queryStr);
        query.setParameter("value", fieldValue);
        List<T> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException(String.format("Multiple rows found with field %s equaling value %s", fieldName,
                    fieldValue.toString()));
        }
        return results.get(0);
    }

    @SuppressWarnings("unchecked")
    public <F> List<T> findAllByField(String fieldName, F fieldValue) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where %s = :value", getEntityClass().getSimpleName(), fieldName);
        Query query = session.createQuery(queryStr);
        query.setParameter("value", fieldValue);
        List<T> results = query.list();
        if (results.size() == 0) {
            return Collections.emptyList();
        }
        return results;
    }

    public final T findByFields(Object... fieldsAndValues) {
        List<T> results = findAllByFields(fieldsAndValues);
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException("Multiple rows found");
        }
        return results.get(0);
    }

    @SuppressWarnings("unchecked")
    public final List<T> findAllByFields(Object... fieldsAndValues) {
        if (fieldsAndValues.length % 2 != 0) {
            throw new RuntimeException("Must specify a value for each field name");
        }

        Session session = getSessionFactory().getCurrentSession();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldsAndValues.length - 1; i += 2) {
            if (i > 0) {
                sb.append(" and ");
            }
            sb.append(String.format("%1$s = :%1$s", fieldsAndValues[i]));
        }
        String queryStr = String.format("from %s where %s", getEntityClass().getSimpleName(), sb.toString());
        Query query = session.createQuery(queryStr);
        for (int i = 0; i < fieldsAndValues.length - 1; i += 2) {
            query.setParameter(fieldsAndValues[i].toString(), fieldsAndValues[i + 1]);
        }
        return query.list();
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
        setAuditingFields(entity);
        getSessionFactory().getCurrentSession().update(entity);
    }

    @Override
    public void delete(T entity) {
        // This is needed as part of Hibernate and JPA integration for backward
        // compatibility
        // Refer to section 5.4 and 5.7 in
        // https://docs.jboss.org/hibernate/orm/5.2/userguide/html_single/Hibernate_User_Guide.html
        if (entity == null) {
            return;
        }
        Session currSession = getSessionFactory().getCurrentSession();
        currSession.delete(currSession.contains(entity) ? entity : currSession.merge(entity));
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

    private void setAuditingFields(T entity) {
        if (entity instanceof HasAuditingFields) {
            Date now = new Date();
            if (entity.getPid() == null) {
                ((HasAuditingFields) entity).setCreated(now);
            }
            ((HasAuditingFields) entity).setUpdated(now);
        }
    }
}
