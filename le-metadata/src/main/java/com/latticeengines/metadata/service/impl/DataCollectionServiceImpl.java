package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.service.DataCollectionService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        return dataCollectionEntityMgr.findAll();
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        return dataCollectionEntityMgr.getDataCollection(type);
    }

    @Override
    public DataCollection createDataCollection(String customerSpace,
            DataCollection dataCollection) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        return dataCollectionEntityMgr.createDataCollection(dataCollection);
    }
}
