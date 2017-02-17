package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.metadata.dao.DataCollectionDao;

@Component("dataCollectionDao")
public class DataCollectionDaoImpl extends BaseDaoImpl<DataCollection> implements DataCollectionDao {
    @Override
    protected Class<DataCollection> getEntityClass() {
        return DataCollection.class;
    }
}
