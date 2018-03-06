package com.latticeengines.metadata.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionServiceImpl.class);

    private ExecutorService attrRepoPool = ThreadPoolUtils.getCachedThreadPool("attr-repo");

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Resource(name = "localCacheService")
    private CacheService localCacheService;

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
        cacheService.refreshKeysByPattern(CustomerSpace.parse(customerSpace).getTenantId(),
                CacheName.getCdlCacheGroup());
        localCacheService.refreshKeysByPattern(CustomerSpace.parse(customerSpace).getTenantId(),
                CacheName.getCdlLocalCacheGroup());
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

        List<String> existingTableNames = dataCollectionEntityMgr.getTableNamesOfRole(collectionName, role, version);
        for (String existingTableName : existingTableNames) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName);
            if (!existingTableName.equals(tableName)) {
                int numLinks = dataCollectionEntityMgr.getTablesFromCollection(collectionName, existingTableName).size();
                removeTable(customerSpace, collectionName, existingTableName, role, version);
                if (numLinks == 1) {
                    new Thread(() -> {
                        log.info(existingTableName + " is an orphan table, delete it completely.");
                        tableEntityMgr.deleteTableAndCleanupByName(existingTableName);
                    }).start();
                }
            }
        }
        log.info("Add table " + tableName + " to collection " + collectionName + " as " + role);
        dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName, role, version);
    }

    @Override
    public void upsertTables(String customerSpace, String collectionName, String[] tableNames,
            TableRoleInCollection role, DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            throw new IllegalArgumentException("Must specify data collection version.");
        }

        Set<String> tableNameSet = new HashSet<>(Arrays.asList(tableNames));
        List<String> existingTableNames = dataCollectionEntityMgr.getTableNamesOfRole(collectionName, role, version);
        for (String existingTableName : existingTableNames) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName);
            if (!tableNameSet.contains(existingTableName)) {
                int numLinks = dataCollectionEntityMgr.getTablesFromCollection(collectionName, existingTableName)
                        .size();
                removeTable(customerSpace, collectionName, existingTableName, role, version);
                if (numLinks == 1) {
                    new Thread(() -> {
                        log.info(existingTableName + " is an orphan table, delete it completely.");
                        tableEntityMgr.deleteTableAndCleanupByName(existingTableName);
                    }).start();
                }
            }
        }

        for (String tableName : tableNames) {
            Table table = tableEntityMgr.findByName(tableName);
            if (table == null) {
                throw new IllegalArgumentException(
                        "Cannot find table named " + tableName + " for customer " + customerSpace);
            }

            log.info("Add table " + tableName + " to collection " + collectionName + " as " + role);
            dataCollectionEntityMgr.upsertTableToCollection(collectionName, tableName, role, version);
        }
    }

    @Override
    public void removeTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection role,
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

        List<String> existingTableNames = dataCollectionEntityMgr.getTableNamesOfRole(collectionName, role, version);
        for (String existingTableName : existingTableNames) {
            if (existingTableName.equals(tableName)) {
                log.info("Removing " + tableName + " as " + role + " in " + version + " from collection.");
                dataCollectionEntityMgr.removeTableFromCollection(collectionName, existingTableName, version);
            }
        }
    }

    @Override
    public void resetTable(String customerSpace, String collectionName, TableRoleInCollection role) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }

        List<String> existingTableNames = dataCollectionEntityMgr.getTableNamesOfRole(collectionName, role, null);
        for (String existingTableName : existingTableNames) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName
                    + ". Remove it from collection and delete it.");
            dataCollectionEntityMgr.removeTableFromCollection(collectionName, existingTableName, null);
            tableEntityMgr.deleteTableAndCleanupByName(existingTableName);
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
    public void removeStats(String customerSpace, String collectionName, DataCollection.Version version) {
        StatisticsContainer container = getStats(customerSpace, collectionName, version);
        if (container != null) {
            log.info("Removing stats in collection " + collectionName + " at version " + version);
            statisticsContainerEntityMgr.delete(container);
        }
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

    @Override
    public List<String> getTableNames(String customerSpace, String collectionName, TableRoleInCollection tableRole,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getOrCreateDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            // by default get active version
            version = dataCollectionEntityMgr.getActiveVersion();
        }
        log.info("Getting all table names of role " + tableRole + " in collection " + collectionName);
        return dataCollectionEntityMgr.getTableNamesOfRole(collectionName, tableRole, version);
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

        Map<String, StatsCube> statsCubes = statisticsContainer.getStatsCubes();
        if (MapUtils.isNotEmpty(statsCubes)) {
            return constructAttrRepoByStatsCubes(customerSpace, notNullCollectionName, statsCubes, version);
        } else {
            return null;
        }
    }

    private AttributeRepository constructAttrRepoByStatsCubes(String customerSpace, String collectionName,
            Map<String, StatsCube> statsCubes, DataCollection.Version version) {
        List<TableRoleInCollection> roles = extractServingRoles(statsCubes);
        Map<TableRoleInCollection, Table> tableMap = constructRoleTableMap(customerSpace, collectionName, roles,
                version);
        AttributeRepository attrRepo = AttributeRepository.constructRepo(statsCubes, tableMap,
                CustomerSpace.parse(customerSpace), collectionName);
        List<Table> productTables = getTables(customerSpace, collectionName, BusinessEntity.Product.getServingStore(),
                version);
        if (productTables != null && !productTables.isEmpty()) {
            Table productTable = productTables.get(0);
            attrRepo.appendServingStore(BusinessEntity.Product, productTable);
        }
        return attrRepo;
    }

    private Map<TableRoleInCollection, Table> constructRoleTableMap(String customerSpace, String collectionName,
            List<TableRoleInCollection> roles, DataCollection.Version version) {
        Map<TableRoleInCollection, Table> tableMap = new HashMap<>();
        Map<TableRoleInCollection, Future<Table>> futureMap = new HashMap<>();
        if (attrRepoPool == null) {
            attrRepoPool = ThreadPoolUtils.getFixedSizeThreadPool("attr-repo", 4);
        }
        Tenant tenantInContext = MultiTenantContext.getTenant();
        roles.forEach(role -> futureMap.put(role, attrRepoPool.submit(() -> {
            MultiTenantContext.setTenant(tenantInContext);
            List<Table> tables = getTables(customerSpace, collectionName, role, version);
            if (tables != null && !tables.isEmpty()) {
                return tables.get(0);
            } else {
                return null;
            }
        })));
        futureMap.forEach((role, future) -> {
            Table table;
            try {
                table = future.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (table != null) {
                tableMap.put(role, table);
            }
        });
        return tableMap;
    }

    private List<TableRoleInCollection> extractServingRoles(Map<String, StatsCube> statsCubes) {
        Set<BusinessEntity> entitySet = statsCubes.keySet().stream() //
                .map(BusinessEntity::valueOf).collect(Collectors.toSet());
        if (entitySet.contains(BusinessEntity.PurchaseHistory)) {
            entitySet.add(BusinessEntity.Transaction);
            entitySet.add(BusinessEntity.PeriodTransaction);
            // TODO: do not remove after M18
            entitySet.remove(BusinessEntity.PurchaseHistory);
        }
        return entitySet.stream().map(BusinessEntity::getServingStore).collect(Collectors.toList());
    }

    @Override
    public String updateDataCloudBuildNumber(String customerSpace, String collectionName, String dataCloudBuildNumber) {
        DataCollection collection = getDataCollection(customerSpace, collectionName);
        collection.setDataCloudBuildNumber(dataCloudBuildNumber);
        log.info("Setting DataCloudBuildNumber of " + collection.getName() + " in " + customerSpace + " to " + dataCloudBuildNumber);
        dataCollectionEntityMgr.update(collection);
        return collection.getDataCloudBuildNumber();
    }

}
