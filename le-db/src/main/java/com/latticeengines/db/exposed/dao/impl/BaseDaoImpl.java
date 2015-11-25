package com.latticeengines.db.exposed.dao.impl;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Provides persistence with the single converged platform default multi-tenant
 * database. Also supports projects with at most one custom sessionFactory in
 * spring context. This is the default baseDao to inherit from and should be
 * used in most cases.
 */
@Repository
public abstract class BaseDaoImpl<T extends HasPid> extends AbstractBaseDaoImpl<T> implements BaseDao<T> {

    @Autowired
    protected SessionFactory sessionFactory;

    @Override
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

    protected BaseDaoImpl() {
    }

}
