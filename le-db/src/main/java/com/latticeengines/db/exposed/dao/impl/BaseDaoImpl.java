package com.latticeengines.db.exposed.dao.impl;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Repository
public abstract class BaseDaoImpl<T extends HasPid>
        extends BaseDaoWithAssignedSessionFactoryImpl<T> implements BaseDao<T> {

    @Autowired
    protected SessionFactory sessionFactory;
}
