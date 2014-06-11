package com.latticeengines.dataplatform.dao.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Repository
public abstract class BaseDaoImpl<T extends HasPid> implements BaseDao<T> {

    private PropertiesConfiguration store = null;

    /**
     * a Class presentation of the entity object that the subclass Dao is
     * working with
     */
    abstract protected Class<T> getEntityClass();

    @Autowired
    protected SessionFactory sessionFactory;

    BaseDaoImpl() {
        String fileName = getFileName();
        File f = new File(fileName);

        try {
            f.createNewFile();
            store = new PropertiesConfiguration();
            store.setDelimiterParsingDisabled(true);
            store.setFile(f);
            store.load();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_14000, e, new String[] { fileName });
        }
    }

    protected String getFileName() {
        return getClass().getName() + ".properties";
    }

    protected PropertiesConfiguration getStore() {
        return store;
    }

    /**
     * this is a generic create for the ORM layer. this should work for all
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
    public boolean containInSession(T entity)  {
        boolean bContains = sessionFactory.getCurrentSession().contains(entity);
        
        return bContains; 
    }

    /**
     * Either create(Object) or update(Object) the given instance, depending upon
     * resolution of the unsaved-value checks (see the manual for discussion of
     * unsaved-value checking). This operation cascades to associated instances
     * if the association is mapped with cascade="save-update"
     * 
     * 
     * @param entity - Parameters: object - a transient or detached instance containing new or
     * updated state
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
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public T findByKey(T entity) {
        Class clz = entity.getClass();

        T e = (T) sessionFactory.getCurrentSession().get(clz, entity.getPid());
        return e;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public T findByKey(Class<T> entityClz, Long key) {
        return (T) sessionFactory.getCurrentSession().get(entityClz, key);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<T> findAll() {
        Session session = sessionFactory.getCurrentSession();
        Class<T> entityClz = getEntityClass();
        Query query = session.createQuery("from " + entityClz.getSimpleName());
        List<T> entities = query.list();

        return entities;
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

    // @Override
    public void load() {
        String fileName = getFileName();
        try {
            store.load(fileName);
        } catch (ConfigurationException e) {
            throw new LedpException(LedpCode.LEDP_14001, e, new String[] { fileName });
        }
    }

    // @Override
    public void save() {
        try {
            store.save();
        } catch (ConfigurationException e) {
            throw new LedpException(LedpCode.LEDP_14002, e, new String[] { getFileName() });
        }
    }

    // @Override
    public void post(T entity) {
        store.setProperty(entity.getPid().toString(), serialize(entity));
    }

    // @Override
    public void clear() {
        store.clear();
    } 

    // @Override
    public void deleteStoreFile() {
        clear();
        File f = new File(getFileName());
        f.delete();
    }

    @Override
    public String serialize(T entity) {
        return entity.toString();
    } 
}
