package com.latticeengines.db.exposed.dao.impl;

import org.hibernate.SessionFactory;
import org.springframework.stereotype.Repository;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

/**
 * Extend from this when there are 2 or more custom session factories in a
 * spring context.
 */
@Repository
public abstract class BaseDaoWithAssignedSessionFactoryImpl<T extends HasPid> extends AbstractBaseDaoImpl<T> implements
        BaseDao<T> {

    protected SessionFactory sessionFactory;

    @Override
    protected SessionFactory getSessionFactory() {
        return this.sessionFactory;
    }

    public void setSessionFactory(SessionFactory factory) {
        this.sessionFactory = factory;
    }

    protected BaseDaoWithAssignedSessionFactoryImpl() {
    }

}
