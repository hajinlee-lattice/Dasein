package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.SegmentationDataCollectionService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Log log = LogFactory.getLog(DataCollectionServiceImpl.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SegmentationDataCollectionService segmentationDataCollectionService;

    @Autowired
    private TableTagEntityMgr tableTagEntityMgr;

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        return dataCollectionEntityMgr.findAll();
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        DataCollection collection = dataCollectionEntityMgr.getDataCollection(type);
        fillInTables(collection);
        return collection;
    }

    @Override
    public DataCollection createDataCollection(String customerSpace, DataCollection dataCollection) {
        tableTypeHolder.setTableType(TableType.DATATABLE);
        DataCollection collection = dataCollectionEntityMgr.createDataCollection(dataCollection);
        fillInTables(collection);
        return collection;
    }

    private void fillInTables(DataCollection dataCollection) {
        List<Table> tables = tableTagEntityMgr.getTablesForTag(dataCollection.getName());
        dataCollection.setTables(tables);
        fillInDefaultTables(dataCollection);
    }

    private void fillInDefaultTables(DataCollection dataCollection) {
        if (dataCollection.getType() == DataCollectionType.Segmentation) {
            segmentationDataCollectionService.fillInDefaultTables(dataCollection);
        }
    }
}
