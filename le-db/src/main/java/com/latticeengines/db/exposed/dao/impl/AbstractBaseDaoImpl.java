package com.latticeengines.db.exposed.dao.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.math.NumberUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Environment;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate5.SessionFactoryUtils;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SoftDeletable;

public abstract class AbstractBaseDaoImpl<T extends HasPid> implements BaseDao<T> {

    protected static final int DEFAULT_JDBC_BATCH_SIZE = 50;
    private static final Logger log = LoggerFactory.getLogger(AbstractBaseDaoImpl.class);

    protected abstract SessionFactory getSessionFactory();

    /**
     * Class presentation of the entity object that the subclass Dao is working
     * with.
     */
    protected abstract Class<T> getEntityClass();

    protected Session getCurrentSession() {
        return getSessionFactory().getCurrentSession();
    }
    
    @Deprecated
    // This is a temporary workaround to get the entity class from outside
    // When we migrate to pure JPA, we will delete this method
    public Class<T> getEntityClassReference() {
        return getEntityClass();
    }

    /**
     * This is a generic create for the ORM layer. This should work for all
     * entity types.
     */
    @Override
    public void create(T entity) {
        setAuditingFields(entity);
        getCurrentSession().persist(entity);
    }

    @Override
    public void create(Collection<T> entities) {
        create(entities, true);
    }

    @Override
    public void create(Collection<T> entities, boolean setAuditFields) {
        Session session = getCurrentSession();
        int batchSize = getBatchSize();

        Iterator<T> iterator = entities.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            if (i > 0 && i % batchSize == 0) {
                //flush a batch of inserts and release memory
                session.flush();
                session.clear();
            }

            T entity = iterator.next();
            if (setAuditFields) {
                setAuditingFields(entity);
            }
            session.persist(entity);
        }
    }

    @Override
    public boolean containInSession(T entity) {
        boolean bContains = getCurrentSession().contains(entity);

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
        getCurrentSession().saveOrUpdate(entity);
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

        return (T) getCurrentSession().get(clz, entity.getPid());
    }

    @Override
    public T findByKey(Class<T> entityClz, Long key) {
        return (T) getCurrentSession().get(entityClz, key);
    }

    @SuppressWarnings("unchecked")
    public <F> T findByField(String fieldName, F fieldValue) {
        Session session = getCurrentSession();
        String queryStr = String.format("from %s where %s = :value", getEntityClass().getSimpleName(), fieldName);
        Query<T> query = session.createQuery(queryStr);
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
        Session session = getCurrentSession();
        String queryStr = String.format("from %s where %s = :value", getEntityClass().getSimpleName(), fieldName);
        Query<T> query = session.createQuery(queryStr);
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

        Session session = getCurrentSession();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldsAndValues.length / 2; i++) {
            if (i > 0) {
                sb.append(" and ");
            }
            sb.append(String.format("%s = ?%d", fieldsAndValues[2 * i], i + 1));
        }
        String queryStr = String.format("from %s where %s", getEntityClass().getSimpleName(), sb.toString());
        Query<T> query = session.createQuery(queryStr);
        for (int i = 0; i < fieldsAndValues.length / 2; i++) {
            query.setParameter(i + 1, fieldsAndValues[2 * i + 1]);
        }
        return query.list();
    }

    @SuppressWarnings("unchecked")
    public final List<T> findAllSortedByFieldWithPagination(int offset, int limit,
            String sortByField, Object... fieldsAndValues) {
        // verify offset and limit
        if (offset < 0 || limit < 0) {
            throw new IllegalArgumentException("Value of Offset and limit should be >= 0");
        }
        Session session = getCurrentSession();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldsAndValues.length / 2; i++) {
            if (i > 0) {
                sb.append(" and ");
            }
            sb.append(String.format("%s = ?%d", fieldsAndValues[2 * i], i + 1));
        }
        String queryStr = String.format("from %s where %s order by %s",
                getEntityClass().getSimpleName(), sb.toString(), sortByField);
        Query<T> query = session.createQuery(queryStr);
        query.setMaxResults(limit);
        query.setFirstResult(offset);
        for (int i = 0; i < fieldsAndValues.length / 2; i++) {
            query.setParameter(i + 1, fieldsAndValues[2 * i + 1]);
        }
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> findAll() {
        Session session = getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query<T> query = session.createQuery("from " + entityClz.getSimpleName());
        return query.list();
    }

    @Override
    public void update(T entity) {
        setAuditingFields(entity);
        Session currSession = getCurrentSession();
        currSession.update(entity);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T merge(T entity) {
        setAuditingFields(entity);
        return (T) getCurrentSession().merge(entity);
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
        Session currSession = getCurrentSession();
        currSession.delete(currSession.contains(entity) ? entity : currSession.merge(entity));
    }

    @Override
    public void deleteByName(String name, boolean hardDelete) {
        if (hardDelete) {
            hardDelete("name", name);
        } else {
            softDelete("name", name);
        }
    }

    @Override
    public void deleteById(String id, boolean hardDelete) {
        if (hardDelete) {
            hardDelete("id", id);
        } else {
            softDelete("id", id);
        }
    }

    @Override
    public void deleteByPid(Long pid, boolean hardDelete) {
        if (hardDelete) {
            hardDelete("pid", pid);
        } else {
            softDelete("pid", pid);
        }
    }

    private void softDelete(String field, Object id) {
        updateDeleted(field, id, true);
    }

    private void hardDelete(String field, Object id) {
        log.info(String.format("Delete entry of %s with %s = %s", getEntityClass().getSimpleName(), field, id));
        Session session = getCurrentSession();
        Query<?> query = session
                .createQuery("delete from " + getEntityClass().getSimpleName() + " where " + field + "= :id")
                .setParameter("id", id);
        query.executeUpdate();
    }

    @Override
    public void revertDeleteById(String id) {
        revertDelete("id", id);
    }

    @Override
    public void revertDeleteByPid(Long pid) {
        revertDelete("pid", pid);
    }

    @Override
    public void revertDeleteByName(String name) {
        revertDelete("name", name);
    }

    private void revertDelete(String field, Object id) {
        updateDeleted(field, id, false);
    }

    private void updateDeleted(String field, Object id, boolean deleted) {
        Session session = getCurrentSession();
        if (SoftDeletable.class.isAssignableFrom(getEntityClass())) {
            Query<?> query = session.createQuery(
                    "update " + getEntityClass().getSimpleName() + " set DELETED = :deleted where " + field + "= :id")
                    .setParameter("id", id).setParameter("deleted", deleted);
            query.executeUpdate();
        } else {
            throw new LedpException(LedpCode.LEDP_50000, new String[] { getEntityClass().getSimpleName() });
        }
    }

    @Override
    public void deleteAll() {
        Session session = getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query<?> query = session.createQuery("delete from " + entityClz.getSimpleName());
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

    @Override
    public void flushSession() {
        getCurrentSession().flush();
    }

    @Override
    public void clearSession() {
        getCurrentSession().clear();
    }
    
    @Override
    public int getBatchSize() {
        Object batch_size = getSessionFactory().getProperties().get(Environment.STATEMENT_BATCH_SIZE);
        return NumberUtils.toInt(String.valueOf(batch_size), DEFAULT_JDBC_BATCH_SIZE);
    }
}
