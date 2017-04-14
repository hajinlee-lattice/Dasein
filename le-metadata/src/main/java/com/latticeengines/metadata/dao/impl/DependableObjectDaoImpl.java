package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.metadata.dao.DependableObjectDao;

@Component("dependableObjectDao")
public class DependableObjectDaoImpl extends BaseDaoImpl<DependableObject> implements DependableObjectDao {
    @Override
    protected Class<DependableObject> getEntityClass() {
        return DependableObject.class;
    }
}
