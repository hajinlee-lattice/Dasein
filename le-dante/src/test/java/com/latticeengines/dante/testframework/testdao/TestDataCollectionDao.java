package com.latticeengines.dante.testframework.testdao;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;

@Component("testDataCollectionDao")
public class TestDataCollectionDao extends BaseDaoImpl<DataCollection> {
    @Override
    protected Class<DataCollection> getEntityClass() {
        return DataCollection.class;
    }
}
