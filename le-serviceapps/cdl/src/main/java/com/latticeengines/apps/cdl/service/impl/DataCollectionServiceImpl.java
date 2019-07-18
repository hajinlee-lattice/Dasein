package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.util.IOUtils;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionArtifactEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusHistoryEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.util.DiagnoseTable;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace;
import com.latticeengines.domain.exposed.cdl.CDLDataSpace.TableSpace;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Logger log = LoggerFactory.getLogger(DataCollectionServiceImpl.class);

    private ExecutorService attrRepoPool = ThreadPoolUtils.getCachedThreadPool("attr-repo");

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private DataCollectionStatusEntityMgr dataCollectionStatusEntityMgr;

    @Inject
    private DataCollectionStatusHistoryEntityMgr dataCollectionStatusHistoryEntityMgr;

    @Inject
    private DataCollectionArtifactEntityMgr dataCollectionArtifactEntityMgr;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Inject
    private S3Service s3Service;

    @Inject
    private BatonService batonService;

    @Resource(name = "localCacheService")
    private CacheService localCacheService;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    @Override
    public DataCollection getDataCollection(String customerSpace, String collectionName) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        return dataCollectionEntityMgr.getDataCollection(collectionName);
    }

    @Override
    public DataCollection.Version getActiveVersion(String customerSpace) {
        DataCollection dataCollection = getDataCollection(customerSpace, "");
        return dataCollection.getVersion();
    }

    @Override
    public DataCollection.Version switchDataCollectionVersion(String customerSpace, String collectionName,
            DataCollection.Version version) {
        DataCollection collection = getDataCollection(customerSpace, collectionName);
        collection.setVersion(version);
        log.info("Switching " + collection.getName() + " in " + customerSpace + " to " + version);
        dataCollectionEntityMgr.update(collection);
        DataCollection.Version newVersion = getDataCollection(customerSpace, collectionName).getVersion();
        clearCache(customerSpace);
        return newVersion;
    }

    @Override
    public DataCollection getDefaultCollection(String customerSpace) {
        return dataCollectionEntityMgr.findDefaultCollection();
    }

    @Override
    public void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getDefaultCollection(customerSpace);
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

        List<String> existingTableNames = dataCollectionEntityMgr.findTableNamesOfRole(collectionName, role, version);
        for (String existingTableName : existingTableNames) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName);
            if (!existingTableName.equals(tableName)) {
                int numLinks = dataCollectionEntityMgr.findTablesFromCollection(collectionName, existingTableName)
                        .size();
                removeTable(customerSpace, collectionName, existingTableName, role, version);
                Tenant currentTenant = MultiTenantContext.getTenant();
                if (numLinks == 1) {
                    new Thread(() -> {
                        MultiTenantContext.setTenant(currentTenant);
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
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            throw new IllegalArgumentException("Must specify data collection version.");
        }

        Set<String> tableNameSet = new HashSet<>(Arrays.asList(tableNames));
        List<String> existingTableNames = dataCollectionEntityMgr.findTableNamesOfRole(collectionName, role, version);
        for (String existingTableName : existingTableNames) {
            log.info("There are already table(s) of role " + role + " in data collection " + collectionName);
            if (!tableNameSet.contains(existingTableName)) {
                int numLinks = dataCollectionEntityMgr.findTablesFromCollection(collectionName, existingTableName)
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
            DataCollection collection = getDefaultCollection(customerSpace);
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

        List<String> existingTableNames = dataCollectionEntityMgr.findTableNamesOfRole(collectionName, role, version);
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
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }

        List<String> existingTableNames = dataCollectionEntityMgr.findTableNamesOfRole(collectionName, role, null);
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
            DataCollection collection = getDefaultCollection(customerSpace);
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
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            // by default get active version
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        return statisticsContainerEntityMgr.findInMasterSegment(collectionName, version);
    }

    @Override
    public List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            // by default get active version
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        log.info("Getting all tables of role " + tableRole + " in collection " + collectionName);
        return dataCollectionEntityMgr.findTablesOfRole(collectionName, tableRole, version);
    }

    @Override
    public List<String> getTableNames(String customerSpace, String collectionName, TableRoleInCollection tableRole,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        if (version == null) {
            // by default get active version
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        return dataCollectionEntityMgr.findTableNamesOfRole(collectionName, tableRole, version);
    }

    @Override
    public List<String> getAllTableNames() {
        return dataCollectionEntityMgr.getAllTableName();
    }

    @Override
    public Map<TableRoleInCollection, Map<Version, List<Table>>> getTableRoleMap(String customerSpace, String collectionName){
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getDefaultCollection(customerSpace);
            collectionName = collection.getName();
        }
        Map<TableRoleInCollection, Map<Version, List<String>>> tableRoleNames = dataCollectionEntityMgr.findTableNamesOfAllRole(collectionName, null, null);
        Map<String,Table> tableMap = new HashMap<>();
        for(Version version:Version.values()) {
            List<Table> tableList = getTables(customerSpace, null, null, version);
            for (Table table:tableList){
                tableMap.put(table.getName(),table);
            }
        }
        return createTableRoleMap(tableRoleNames,tableMap);
    }

    private Map<TableRoleInCollection, Map<Version, List<Table>>> createTableRoleMap(
            Map<TableRoleInCollection, Map<Version, List<String>>> tableRoleNames, Map<String,Table> tableMap) {
        Map<TableRoleInCollection, Map<Version, List<Table>>> tableRoleMap = new HashMap<>();
        for (Map.Entry<TableRoleInCollection, Map<Version, List<String>>> entry:tableRoleNames.entrySet()) {
            TableRoleInCollection tableRole = entry.getKey();
            if (!tableRoleMap.containsKey(tableRole)){
                tableRoleMap.put(tableRole,new HashMap<>());
            }
            Map<Version,List<String>> verTNameMap = tableRoleNames.get(tableRole);
            for (DataCollection.Version version:verTNameMap.keySet()) {
                tableRoleMap.get(tableRole).put(version,new ArrayList<>());
                for (String tableName:verTNameMap.get(version)) {
                    tableRoleMap.get(tableRole).get(version).add(tableMap.get(tableName));
                }
            }
        }
        return tableRoleMap;
    }

    public AttributeRepository getAttrRepo(String customerSpace, String collectionName,
            DataCollection.Version version) {
        if (StringUtils.isBlank(collectionName)) {
            DataCollection collection = getDefaultCollection(customerSpace);
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
        addEntityToAttrRepo(attrRepo, BusinessEntity.Product, customerSpace, collectionName, version);
        addEntityToAttrRepo(attrRepo, BusinessEntity.DepivotedPurchaseHistory, customerSpace, collectionName, version);
        return attrRepo;
    }

    private void addEntityToAttrRepo(AttributeRepository attrRepo, BusinessEntity entity, String customerSpace,
            String collectionName, DataCollection.Version version) {
        List<Table> productTables = getTables(customerSpace, collectionName, entity.getServingStore(), version);
        if (productTables != null && !productTables.isEmpty()) {
            Table productTable = productTables.get(0);
            attrRepo.appendServingStore(entity, productTable);
        }
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
            entitySet.remove(BusinessEntity.PurchaseHistory);
        }
        return entitySet.stream().map(BusinessEntity::getServingStore).collect(Collectors.toList());
    }

    @Override
    @Deprecated
    public String updateDataCloudBuildNumber(String customerSpace, String collectionName, String dataCloudBuildNumber) {
        DataCollection collection = getDataCollection(customerSpace, collectionName);
        collection.setDataCloudBuildNumber(dataCloudBuildNumber);
        log.info("Setting DataCloudBuildNumber of " + collection.getName() + " in " + customerSpace + " to "
                + dataCloudBuildNumber);
        dataCollectionEntityMgr.update(collection);
        return collection.getDataCloudBuildNumber();
    }

    @Override
    @NoCustomerSpace
    public void clearCache(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(tenantId, CacheName.getCdlCacheGroup());
        localCacheService.refreshKeysByPattern(tenantId, CacheName.getCdlLocalCacheGroup());
    }

    @Override
    public DataCollectionStatus getOrCreateDataCollectionStatus(String customerSpace, DataCollection.Version version) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (version == null) {
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        DataCollectionStatus status = dataCollectionStatusEntityMgr.findByTenantAndVersion(tenant, version);
        if (status == null) {
            DataCollection collection = getDefaultCollection(customerSpace);
            status = new DataCollectionStatus();
            status.setDataCollection(collection);
            status.setTenant(tenant);
            status.setVersion(version);
        }
        return status;
    }

    @Override
    public void saveOrUpdateStatus(String customerSpace, DataCollectionStatus status,
            DataCollection.Version version) {
        DataCollectionStatus currentStatus = getOrCreateDataCollectionStatus(customerSpace, version);
        currentStatus.setDetail(status.getDetail());
        // skip compare new detail with previous detail in DB,PA will get
        // current collection status in DB
        dataCollectionStatusEntityMgr.createOrUpdate(currentStatus);
    }

    @Override
    public void saveStatusHistory(String customerSpace, DataCollectionStatus status) {
        DataCollectionStatusHistory statusHistory = new DataCollectionStatusHistory();
        statusHistory.setTenant(MultiTenantContext.getTenant());
        populateStatusDetailHistory(status, statusHistory);
        dataCollectionStatusHistoryEntityMgr.create(statusHistory);

    }

    @Override
    public List<DataCollectionStatusHistory> getCollectionStatusHistory(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return dataCollectionStatusHistoryEntityMgr.findByTenantOrderByCreationTimeDesc(tenant);
    }

    private void populateStatusDetailHistory(DataCollectionStatus status, DataCollectionStatusHistory statusHistory) {
        statusHistory.setAccountCount(status.getAccountCount());
        statusHistory.setApsRollingPeriod(status.getApsRollingPeriod());
        statusHistory.setContactCount(status.getContactCount());
        statusHistory.setDataCloudBuildNumber(status.getDataCloudBuildNumber());
        statusHistory.setDateMap(status.getDateMap());
        statusHistory.setEvaluationDate(status.getEvaluationDate());
        statusHistory.setMaxTxnDate(status.getMaxTxnDate());
        statusHistory.setMinTxnDate(status.getMinTxnDate());
        statusHistory.setOrphanContactCount(status.getOrphanContactCount());
        statusHistory.setOrphanTransactionCount(status.getOrphanTransactionCount());
        statusHistory.setProductCount(status.getProductCount());
        statusHistory.setTransactionCount(status.getTransactionCount());
        statusHistory.setUnmatchedAccountCount(status.getUnmatchedAccountCount());
    }

    @Override
    public DataCollection createDefaultCollection() {
        return dataCollectionEntityMgr.createDefaultCollection();
    }

    @Override
    public CDLDataSpace createCDLDataSpace(String customerSpace) {
        CDLDataSpace cdlDataSpace = new CDLDataSpace();
        cdlDataSpace.setActiveVersion(getActiveVersion(customerSpace));
        Map<BusinessEntity, Map<BusinessEntity.DataStore, List<TableSpace>>>   entities = new HashMap<>();
        Set<TableRoleInCollection> tableRolesWithEntity = new HashSet<>();
        Map<TableRoleInCollection, Map<Version, List<Table>>> tableRoleNames = getTableRoleMap(customerSpace,null);

        for (BusinessEntity entity : BusinessEntity.values()) {
            Map<BusinessEntity.DataStore, List<TableSpace>> dataStoreMap = new HashMap<>();
            for (BusinessEntity.DataStore dataStore:BusinessEntity.DataStore.values()) {
                List<TableSpace> tableSpaceList = new ArrayList<>();
                for (DataCollection.Version version:DataCollection.Version.values()) {
                    TableRoleInCollection tableRole = getTableRoleFromStore(entity,dataStore);
                    tableRolesWithEntity.add(tableRole);

                    if (MapUtils.isEmpty(tableRoleNames.get(tableRole)))
                        continue;
                    List<Table> tables = tableRoleNames.get(tableRole).get(version);
                    if (CollectionUtils.isEmpty(tables))
                        continue;
                    List<String> tableNames = new ArrayList<>();
                    for (Table table:tables) {
                        tableNames.add(table.getName());
                    }
                    TableSpace tableSpace = createTableSpace(tableNames, tables, dataStore, version);
                    tableSpace.setTableRole(tableRole);
                    tableSpaceList.add(tableSpace);
                }
                if (CollectionUtils.isNotEmpty(tableSpaceList)) {
                    dataStoreMap.put(dataStore, tableSpaceList);
                }
            }
            if (MapUtils.isNotEmpty(dataStoreMap)) {
                entities.put(entity,dataStoreMap);
            }
        }
        cdlDataSpace.setEntities(entities);
        Map<String, List<TableSpace>> others = getOtherTableRoles(tableRolesWithEntity,tableRoleNames);
        cdlDataSpace.setOthers(others);

        return cdlDataSpace;
    }

    @Override
    public List<DataCollectionArtifact> getArtifacts(String customerSpace, DataCollectionArtifact.Status status,
                                                     Version version) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (version == null) {
            version = dataCollectionEntityMgr.findActiveVersion();
        }

        if (status != null) {
            return dataCollectionArtifactEntityMgr.findByTenantAndStatusAndVersion(tenant, status, version);
        } else {
            return dataCollectionArtifactEntityMgr.findByTenantAndVersion(tenant, version);
        }
    }

    @Override
    public DataCollectionArtifact getLatestArtifact(String customerSpace, String name, Version version) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (version == null) {
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        List<DataCollectionArtifact> artifacts =
                dataCollectionArtifactEntityMgr.findByTenantAndNameAndVersion(tenant, name, version);
        if (artifacts != null && artifacts.size() > 0) {
            return artifacts.get(0);
        } else {
            return null;
        }
    }

    @Override
    public DataCollectionArtifact getOldestArtifact(String customerSpace, String name, Version version) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (version == null) {
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        List<DataCollectionArtifact> artifacts =
                dataCollectionArtifactEntityMgr.findByTenantAndNameAndVersion(tenant, name, version);
        if (artifacts != null && artifacts.size() > 0) {
            return artifacts.get(artifacts.size() - 1);
        } else {
            return null;
        }
    }

    @Override
    public DataCollectionArtifact createArtifact(String customerSpace, String artifactName, String artifactUrl,
                                                 DataCollectionArtifact.Status status, DataCollection.Version version) {
        if (version == null) {
            throw new UnsupportedOperationException("Data collection version cannot be null.");
        }

        Tenant tenant = MultiTenantContext.getTenant();
        DataCollection collection = getDefaultCollection(customerSpace);
        DataCollectionArtifact artifact = new DataCollectionArtifact();
        artifact.setDataCollection(collection);
        artifact.setName(artifactName);
        artifact.setUrl(artifactUrl);
        artifact.setTenant(tenant);
        artifact.setStatus(status);
        artifact.setVersion(version);
        artifact.setCreateTime(System.currentTimeMillis());
        dataCollectionArtifactEntityMgr.create(artifact);
        return artifact;
    }

    @Override
    public DataCollectionArtifact updateArtifact(String customerSpace, DataCollectionArtifact artifact) {
        DataCollectionArtifact existing = getLatestArtifact(customerSpace, artifact.getName(), artifact.getVersion());
        if (existing == null) {
            existing = createArtifact(customerSpace, artifact.getName(), artifact.getUrl(), artifact.getStatus(),
                    artifact.getVersion());
        } else {
            existing.setUrl(artifact.getUrl());
            existing.setStatus(artifact.getStatus());
        }
        dataCollectionArtifactEntityMgr.update(existing);
        return existing;
    }

    @Override
    public DataCollectionArtifact deleteArtifact(String customerSpace, String name, DataCollection.Version version,
                                                 boolean deleteLatest) {
        if (version == null) {
            version = getActiveVersion(customerSpace);
        }
        DataCollectionArtifact artifact;
        if (deleteLatest) {
            artifact = getLatestArtifact(customerSpace, name, version);
        } else {
            artifact = getOldestArtifact(customerSpace, name, version);
        }
        log.info(String.format("Deleting artifact %s at version %s in collection.",
                JsonUtils.serialize(artifact), version));
        dataCollectionArtifactEntityMgr.delete(artifact);
        return artifact;
    }

    @Override
    public byte[] downloadDataCollectionArtifact(String customerSpace, String exportId) {
        DataCollection.Version activeVersion = getActiveVersion(customerSpace);
        if (activeVersion == null) {
            throw new RuntimeException("Cannot find active version of data collection for tenant " + customerSpace);
        }
        log.info(String.format("Download data collection artifact. Version=%s, ExportId=%s",
                activeVersion, exportId));
        List<DataCollectionArtifact> artifacts = getArtifacts(customerSpace, DataCollectionArtifact.Status.READY,
                activeVersion).stream()
                .filter(artifact -> artifact.getUrl().contains(exportId))
                .collect(Collectors.toList());
        if (artifacts.isEmpty()) {
            log.info(String.format(
                    "No artifact available for downloading. Tenant=%s, ExportId=%s.", customerSpace, exportId));
            return null;
        }

        DataCollectionArtifact artifact = artifacts.get(0);
        String artifactKey = getArtifactKey(s3Bucket, artifact.getUrl());
        log.info("S3 artifactKey=" + artifactKey);

        InputStream inputStream = s3Service.readObjectAsStream(s3Bucket, artifactKey);
        try {
            if (inputStream != null) {
                return IOUtils.toByteArray(inputStream);
            } else {
                return null;
            }
        } catch (IOException exc) {
            throw new RuntimeException(String.format(
                    "Failed to get content of data collection artifact. Bucket=%s. Object key=%s.",
                    s3Bucket, artifactKey), exc);
        }
    }

    private String getArtifactKey(String s3Bucket, String url) {
        String prefix = "s3a://";
        return url.substring(prefix.length() + s3Bucket.length() + 1);
    }

    private TableRoleInCollection getTableRoleFromStore(BusinessEntity entity, BusinessEntity.DataStore dataStore) {
        switch(dataStore) {
        case Batch:
            if(entity.getBatchStore() != null) {
                return entity.getBatchStore();
            }
            break;
        case Serving:
            if (entity.getServingStore() != null) {
                return entity.getServingStore();
            }
            break;
        default:
            throw new UnsupportedOperationException("Unknown data store " + dataStore);
        }
        return null;
    }

    private TableSpace createTableSpace(List<String> tableNames, List<Table> tables,
                                        BusinessEntity.DataStore dataStore, DataCollection.Version version){
        TableSpace tableSpace = new TableSpace();
        List<String> hdfsPaths = new ArrayList<>();
        if (dataStore == null) {
            for (Table table:tables) {
                if (CollectionUtils.isNotEmpty(table.getExtracts())) {
                    hdfsPaths.add(table.getExtracts().get(0).getPath());
                }
            }
            tableSpace.setHdfsPath(hdfsPaths);
            tableSpace.setTables(tableNames);
            return tableSpace;
        }
        switch (dataStore) {
        case Batch:
            for (Table table:tables) {
                if (CollectionUtils.isNotEmpty(table.getExtracts())) {
                    hdfsPaths.add(table.getExtracts().get(0).getPath());
                }
            }
            tableSpace.setHdfsPath(hdfsPaths);
            break;
        case Serving:
            tableSpace.setTables(tableNames);
            break;
        default:
            throw new UnsupportedOperationException("Unknown data store " + dataStore);
        }
        tableSpace.setVersion(version);
        return tableSpace;
    }

    private Map<String, List<TableSpace>> getOtherTableRoles(Set<TableRoleInCollection> tableRolesWithEntity,
                                                             Map<TableRoleInCollection, Map<Version, List<Table>>> tableRoleNames){
        Map<String, List<TableSpace>> others = new HashMap<>();
        for (TableRoleInCollection tableRole:TableRoleInCollection.values()) {
            if (!tableRolesWithEntity.contains(tableRole)) {
                List<TableSpace> tableSpaceList = new ArrayList<>();
                for(DataCollection.Version version:DataCollection.Version.values()) {
                    if (MapUtils.isEmpty(tableRoleNames.get(tableRole)))
                        continue;
                    List<Table> tables = tableRoleNames.get(tableRole).get(version);
                    if (CollectionUtils.isEmpty(tables))
                        continue;
                    List<String> tableNames = new ArrayList<>();
                    for(Table table:tables){
                        tableNames.add(table.getName());
                    }
                    TableSpace tableSpace = createTableSpace(tableNames, tables, null, version);
                    tableSpace.setTableRole(tableRole);
                    tableSpaceList.add(tableSpace);
                }
                if (CollectionUtils.isNotEmpty(tableSpaceList)) {
                    others.put(tableRole.toString(),tableSpaceList);
                }
            }
        }
        return others;
    }

    @Override
    public ImportTemplateDiagnostic diagnostic(String customerSpaceStr, Long dataCollectionTablePid) {
        DataCollectionTable dataCollectionTable = dataCollectionEntityMgr.findDataCollectionTableByPid(dataCollectionTablePid);
        if (dataCollectionTable == null) {
            throw new RuntimeException("Cannot find datacollection table for id: " + dataCollectionTablePid);
        }
        Table table = dataCollectionTable.getTable();
        if (table == null) {
            throw new RuntimeException(String.format("The table for datacollection table %s is empty!",
                    dataCollectionTablePid));
        }
        return DiagnoseTable.diagnostic(customerSpaceStr, table, null, batonService);
    }

}
