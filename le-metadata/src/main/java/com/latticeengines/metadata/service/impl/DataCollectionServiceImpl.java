package com.latticeengines.metadata.service.impl;

import java.util.List;
import java.util.UUID;

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
    public DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection) {
        DataCollection existing = dataCollectionEntityMgr.getDataCollection(dataCollection.getName());
        if (existing != null) {
            dataCollectionEntityMgr.removeDataCollection(existing.getName());
        } else {
            dataCollection.setName("DataCollection_" + UUID.randomUUID());
        }

        dataCollectionEntityMgr.createDataCollection(dataCollection);
        dataCollectionCache.invalidate(dataCollection.getType());
        return dataCollectionCache.get(dataCollection.getType());
    }
}
