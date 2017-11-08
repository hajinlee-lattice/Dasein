package com.latticeengines.metadata.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
    public DataCollection.Version switchDataCollectionVersion(String customerSpace, String collectionName,
            DataCollection.Version version) {
        DataCollection collection = getDataCollection(customerSpace, collectionName);
        collection.setVersion(version);
        log.info("Switching " + collection.getName() + " in " + customerSpace + " to " + version);
        dataCollectionEntityMgr.update(collection);
        DataCollection.Version newVersion = getDataCollection(customerSpace, collectionName).getVersion();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(customerSpace, CacheNames.getCdlProfileCacheGroup());

        return newVersion;
    }

    @Override
    public DataCollection getOrCreateDefaultCollection(String customerSpace) {
        return dataCollectionEntityMgr.getOrCreateDefaultCollection();
    }

    @Override
    public void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }

        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find table named " + tableName + " for customer " + customerSpace);
        }

        if (version == null) {
            throw new IllegalArgumentException("Must specify data collection version.");
        }

        List<Table> existingTables = dataCollectionEntityMgr.getTablesOfRole(collectionName, role, version);
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
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName, role, version);
    }

    @Override
    public void resetTable(String customerSpace, String collectionName, TableRoleInCollection role) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }

        List<Table> existingTables = dataCollectionEntityMgr.getTablesOfRole(collectionName, role, null);
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
    }

    @Override
    public StatisticsContainer getStats(String customerSpace, String collectionName, DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            // by default get active version
            version = dataCollectionEntityMgr.getActiveVersion();
        }
        return statisticsContainerEntityMgr.findInMasterSegment(collectionName, version);
    }

    @Override
    public List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            // by default get active version
            version = dataCollectionEntityMgr.getActiveVersion();
        }
        log.info("Getting all tables of role " + tableRole + " in collection " + collectionName);
        return dataCollectionEntityMgr.getTablesOfRole(collectionName, tableRole, version);
    }

    public AttributeRepository getAttrRepo(String customerSpace, String collectionName,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        final String notNullCollectionName = collectionName;
        StatisticsContainer statisticsContainer = getStats(customerSpace, notNullCollectionName, version);
        if (statisticsContainer == null) {
            return null;
        }
        Statistics statistics = statisticsContainer.getStatistics();
        if (statistics == null) {
            return null;
        }
        List<TableRoleInCollection> roles = extractServingRoles(statistics);
        Map<TableRoleInCollection, Table> tableMap = new HashMap<>();
        roles.forEach(role -> {
            List<Table> tables = getTables(customerSpace, notNullCollectionName, role, version);
            if (tables != null && !tables.isEmpty()) {
                tableMap.put(role, tables.get(0));
            }
        });
        AttributeRepository attrRepo = AttributeRepository.constructRepo(statistics, tableMap,
                CustomerSpace.parse(customerSpace), notNullCollectionName);
        List<Table> productTables = getTables(customerSpace, notNullCollectionName,
                BusinessEntity.Product.getServingStore(), version);
        if (productTables != null && !productTables.isEmpty()) {
            Table productTable = productTables.get(0);
            attrRepo.appendServingStore(BusinessEntity.Product, productTable);
        }
        return attrRepo;
    }

    private List<TableRoleInCollection> extractServingRoles(Statistics statistics) {
        Set<BusinessEntity> entitySet = statistics.getCategories().values().stream()
                .flatMap(cat -> cat.getSubcategories().values().stream()
                        .flatMap(subcat -> subcat.getAttributes().keySet().stream().map(AttributeLookup::getEntity))) //
                .map(entity -> BusinessEntity.PurchaseHistory.equals(entity) ? BusinessEntity.Transaction : entity) //
                .collect(Collectors.toSet());
        return entitySet.stream().map(BusinessEntity::getServingStore).collect(Collectors.toList());
    }

}
