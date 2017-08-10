package com.latticeengines.metadata.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.CustomerMetadata;
import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.CustomerStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionServiceImpl.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Override
    public DataCollection getDataCollection(String customerSpace, String collectionName) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        return dataCollectionEntityMgr.getDataCollection(collectionName);
    }

    @Override
    public DataCollection getOrCreateDefaultCollection(String customerSpace) {
        return dataCollectionEntityMgr.getOrCreateDefaultCollection();
    }

    @Override
    public void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection role) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }

        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find table named " + tableName + " for customer " + customerSpace);
        }

        List<Table> existingTables = dataCollectionEntityMgr.getTablesOfRole(collectionName, role);
        for (Table existingTable : existingTables) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName
                    + ". Remove it from collection and delete it.");
            if (!existingTable.getName().equals(tableName)) {
                log.info("Remove it from collection and delete it.");
                dataCollectionEntityMgr.removeTableFromCollection(collectionName, existingTable.getName());
                tableEntityMgr.deleteTableAndCleanupByName(existingTable.getName());
            }
        }
        log.info("Add table " + tableName + " to collection " + collectionName + " as " + role);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName, role);
        NodeWatcher.updateWatchedData(CustomerMetadata.name(), String.format("%s|%s", customerSpace, role.name()));
    }

    @Override
    public void resetTable(String customerSpace, String collectionName, TableRoleInCollection role) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }

        List<Table> existingTables = dataCollectionEntityMgr.getTablesOfRole(collectionName, role);
        for (Table existingTable : existingTables) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName
                    + ". Remove it from collection and delete it.");
            dataCollectionEntityMgr.removeTableFromCollection(collectionName, existingTable.getName());
            tableEntityMgr.deleteTableAndCleanupByName(existingTable.getName());
        }
    }

    @Override
    public void addStats(String customerSpace, String collectionName, StatisticsContainer container) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        DataCollection dataCollection = getDataCollection(customerSpace, collectionName);
        if (dataCollection == null) {
            throw new IllegalArgumentException(
                    "Cannot find data collection named " + collectionName + " for customer " + customerSpace);
        }
        dataCollectionEntityMgr.upsertStatsForMasterSegment(collectionName, container);
        NodeWatcher.updateWatchedData(CustomerStats.name(), customerSpace);
    }

    @Override
    public StatisticsContainer getStats(String customerSpace, String collectionName) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        return statisticsContainerEntityMgr.findInMasterSegment(collectionName);
    }

    @Override
    public List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        log.info("Getting all tables of role " + tableRole + " in collection " + collectionName);
        return dataCollectionEntityMgr.getTablesOfRole(collectionName, tableRole);
    }

    public AttributeRepository getAttrRepo(String customerSpace, String collectionName) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        final String notNullCollectioName = collectionName;
        StatisticsContainer statisticsContainer = getStats(customerSpace, notNullCollectioName);
        if (statisticsContainer == null) {
            return null;
        }
        Statistics statistics = statisticsContainer.getStatistics();
        if (statistics == null) {
            return null;
        }
        List<TableRoleInCollection> roles = AttributeRepository.extractServingRoles(statistics);
        Map<TableRoleInCollection, Table> tableMap = new HashMap<>();
        roles.forEach(role -> {
            List<Table> tables = getTables(customerSpace, notNullCollectioName, role);
            if (tables != null && !tables.isEmpty()) {
                tableMap.put(role, tables.get(0));
            }
        });
        return AttributeRepository.constructRepo(statistics, tableMap, CustomerSpace.parse(customerSpace),
                notNullCollectioName);
    }

}
