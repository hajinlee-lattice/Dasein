package com.latticeengines.propdata.collection.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.ColumnMapping;
import com.latticeengines.propdata.collection.dao.ColumnMappingDao;

@Component("columnMappingDao")
public class ColumnMappingDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<ColumnMapping> implements ColumnMappingDao {

    @Override
    protected Class<ColumnMapping> getEntityClass() {
        return ColumnMapping.class;
    }

}
