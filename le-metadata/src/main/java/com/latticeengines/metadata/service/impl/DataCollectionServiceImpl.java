package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        return dataCollectionEntityMgr.findAll();
    }

    @Override
    public DataCollection getDefaultDataCollection(String customerSpace) {
        return dataCollectionEntityMgr.getDefaultDataCollection();
    }

    @Override
    public DataCollection createDefaultDataCollection(String customerSpace, String statisticsId, List<String> tableNames) {
        return dataCollectionEntityMgr.createDataCollection(tableNames, statisticsId, true);
    }

    @Override
    public DataCollection getDataCollection(String customerSpace, String dataCollectionName) {
        return dataCollectionEntityMgr.getDataCollection(dataCollectionName);
    }

    @Override
    public DataCollection createDataCollection(String customerSpace, String statisticsId, List<String> tableNames) {
        return dataCollectionEntityMgr.createDataCollection(tableNames, statisticsId, false);
    }
}
