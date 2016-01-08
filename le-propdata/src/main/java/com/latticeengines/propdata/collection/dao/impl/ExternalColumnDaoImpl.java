package com.latticeengines.propdata.collection.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.ExternalColumn;
import com.latticeengines.propdata.collection.dao.ExternalColumnDao;

@Component("externalColumnDao")
public class ExternalColumnDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<ExternalColumn> implements ExternalColumnDao {

    @Override
    protected Class<ExternalColumn> getEntityClass() {
        return ExternalColumn.class;
    }

}
