package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;
import com.latticeengines.metadata.dao.DataCollectionPropertyDao;

@Component("dataCollectionPropertyDao")
public class DataCollectionPropertyDaoImpl extends BaseDaoImpl<DataCollectionProperty>
        implements DataCollectionPropertyDao {

    @Override
    protected Class<DataCollectionProperty> getEntityClass() {
        return DataCollectionProperty.class;
    }

}
