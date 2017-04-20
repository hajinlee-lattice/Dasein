package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.DataCollectionCache;
import com.latticeengines.metadata.service.DataCollectionService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Log log = LogFactory.getLog(DataCollectionServiceImpl.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private DataCollectionCache dataCollectionCache;

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        return dataCollectionEntityMgr.findAll();
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        return dataCollectionCache.get(type);
    }

    @Override
    public DataCollection createDataCollection(String customerSpace, DataCollection dataCollection) {
        DataCollection collection = dataCollectionEntityMgr.createDataCollection(dataCollection);
        dataCollectionEntityMgr.fillInTables(collection);
        dataCollectionCache.invalidate(collection.getType());
        return collection;
    }
}
