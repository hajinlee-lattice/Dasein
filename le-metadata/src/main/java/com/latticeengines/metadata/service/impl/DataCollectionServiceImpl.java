package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.DataCollectionCache;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.security.exposed.service.TenantService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Log log = LogFactory.getLog(DataCollectionServiceImpl.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DataCollectionCache dataCollectionCache;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        return dataCollectionEntityMgr.findAll();
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        return dataCollectionCache.get(type);
    }

    @Override
    public DataCollection getDataCollection(String customerSpace, String collectionName) {
        return dataCollectionEntityMgr.getDataCollection(collectionName);
    }

    @Override
    public DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection) {
        DataCollection existing = dataCollectionEntityMgr.getDataCollection(dataCollection.getName());
        if (existing != null) {
            dataCollectionEntityMgr.removeDataCollection(existing.getName());
        }
        dataCollectionEntityMgr.createDataCollection(dataCollection);
        return getDataCollection(customerSpace, dataCollection.getName());
    }

    @Override
    public void upsertStats(String customerSpace, String collectionName, StatisticsContainer container,
            String modelId) {
        DataCollection dataCollection = getDataCollection(customerSpace, collectionName);
        if (dataCollection == null) {
            throw new IllegalArgumentException(
                    "Cannot find data collection named " + collectionName + " for customer " + customerSpace);
        }
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, container, modelId);
    }

    @Override
    public void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection role) {
        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find table named " + tableName + " for customer " + customerSpace);
        }

        List<Table> existingTables = dataCollectionEntityMgr.getTablesOfRole(collectionName, role);
        for (Table existingTable : existingTables) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName
                    + ". Remove it from collection and delete it.");
            dataCollectionEntityMgr.removeTableFromCollection(collectionName, existingTable.getName());
        }
        log.info("Add table " + tableName + " to collection " + collectionName + " as " + role);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName, role);
    }

    @Override
    public StatisticsContainer getStats(String customerSpace, String collectionName, String modelId) {
        return statisticsContainerEntityMgr.findInMasterSegment(collectionName, modelId);
    }

    @Override
    public List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole) {
        log.info("Getting all tables of role " + tableRole + " in collection " + collectionName);
        return dataCollectionEntityMgr.getTablesOfRole(collectionName, tableRole);
    }

}
