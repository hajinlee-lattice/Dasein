package com.latticeengines.metadata.dao.impl;

import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.dao.AttributeDao;

@Component("attributeDao")
public class AttributeDaoImpl extends BaseDaoImpl<Attribute> implements AttributeDao {

    @Override
    protected Class<Attribute> getEntityClass() {
        return Attribute.class;
    }

    /*
    @Override
    protected Session getCurrentSession() {
        Session locSession = super.getCurrentSession();
        if (locSession != null) {
            locSession.setJdbcBatchSize(DEFAULT_JDBC_BATCH_SIZE);
        }
        return locSession;
    }
    */
    
}
