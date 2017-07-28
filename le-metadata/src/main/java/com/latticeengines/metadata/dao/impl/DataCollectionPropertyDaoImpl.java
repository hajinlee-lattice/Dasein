package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;
import com.latticeengines.metadata.dao.DataCollectionPropertyDao;

@Component("dataCollectionPropertyDao")
public class DataCollectionPropertyDaoImpl extends BaseMetadataPropertyDaoImpl<DataCollectionProperty, DataCollection>
        implements DataCollectionPropertyDao {

    @Override
    protected Class<DataCollectionProperty> getEntityClass() {
        return DataCollectionProperty.class;
    }

}
