package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.DataCollection.Version.Blue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.Tuple;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.dao.DataCollectionDao;
import com.latticeengines.apps.cdl.dao.DataCollectionTableDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.repository.reader.DataCollectionTableReaderRepository;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("dataCollectionEntityMgr")
public class DataCollectionEntityMgrImpl extends BaseEntityMgrImpl<DataCollection> implements DataCollectionEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionEntityMgrImpl.class);

    @Inject
    private DataCollectionDao dataCollectionDao;

    @Inject
    private DataCollectionTableDao dataCollectionTableDao;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionTableReaderRepository dataCollectionTableReaderRepository;

    @Override
    public DataCollectionDao getDao() {
        return dataCollectionDao;
    }

    private MetadataSegment masterSegment(DataCollection dataCollection) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDataCollection(dataCollection);
        segment.setName("Segment_" + UUID.randomUUID().toString().replace("-", ""));
        segment.setDisplayName("Customer Universe");
        segment.setDescription("Master segment of the data collection.");
        segment.setUpdated(new Date());
        segment.setCreated(new Date());
        segment.setMasterSegment(true);
        segment.setTenant(MultiTenantContext.getTenant());
        return segment;
    }

    private DataFeed defaultFeed(DataCollection dataCollection) {
        DataFeed dataFeed = new DataFeed();
        dataFeed.setName(NamingUtils.timestamp("DF"));
        dataFeed.setStatus(DataFeed.Status.Initing);
        dataFeed.setTenant(MultiTenantContext.getTenant());
        dataFeed.setDataCollection(dataCollection);
        return dataFeed;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection getDataCollection(String name) {
        return dataCollectionDao.findByField("name", name);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<Table> findTablesOfRole(String collectionName, TableRoleInCollection tableRole,
                                        DataCollection.Version version) {
        List<String> tableNames = dataCollectionDao.getTableNamesOfRole(collectionName, tableRole, version);
        if (tableNames == null) {
            return Collections.emptyList();
        }
        return tableNames.stream().map(tableEntityMgr::findByName).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<String> findTableNamesOfRole(String collectionName, TableRoleInCollection tableRole,
                                             DataCollection.Version version) {
        List<String> tableNames = dataCollectionDao.getTableNamesOfRole(collectionName, tableRole, version);
        if (tableNames == null) {
            return Collections.emptyList();
        } else {
            return tableNames;
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public Map<String, String> findTableNamesOfOfRoleAndSignatures(String collectionName,
            TableRoleInCollection tableRole, DataCollection.Version version, Set<String> signatures) {
        if (CollectionUtils.isEmpty(signatures)) {
            return Collections.emptyMap();
        }
        // [ signature, tableName ]
        List<Tuple> result = dataCollectionTableReaderRepository.findTablesInRole(collectionName, tableRole, version,
                new ArrayList<>(signatures));
        if (CollectionUtils.isEmpty(result)) {
            return Collections.emptyMap();
        }

        return result.stream().filter(Objects::nonNull) //
                .filter(tuple -> tuple.get(0) instanceof String && tuple.get(1) instanceof String) //
                .map(tuple -> Pair.of((String) tuple.get(0), (String) tuple.get(1))) //
                .filter(pair -> StringUtils.isNotBlank(pair.getRight())) //
                /*-
                 * non-null signature should not have duplicate in role/collection, but just in case
                 */
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight, (v1, v2) -> v1));
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public Map<String, Table> findTablesOfRoleAndSignatures(@NotNull String collectionName,
            @NotNull TableRoleInCollection tableRole, @NotNull DataCollection.Version version, Set<String> signatures) {
        Map<String, String> tableNames = findTableNamesOfOfRoleAndSignatures(collectionName, tableRole, version,
                signatures);
        return tableNames.entrySet().stream() //
                .map(e -> Pair.of(e.getKey(), tableEntityMgr.findByName(e.getValue()))) // get table
                .filter(pair -> pair.getRight() != null) //
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<String> getAllTableName() {
        List<String> tableNames = dataCollectionTableReaderRepository.findAllTableName();
        if (tableNames == null) {
            return Collections.emptyList();
        } else {
            return tableNames;
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public Map<TableRoleInCollection, Map<DataCollection.Version, List<String>>> findTableNamesOfAllRole( //
                                                                                                          String collectionName, TableRoleInCollection tableRole, DataCollection.Version version) {
        return dataCollectionDao.findTableNamesOfAllRole(collectionName, tableRole, version);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataCollectionTable addTableToCollection(@NotNull String collectionName, @NotNull String tableName,
            @NotNull TableRoleInCollection role, String signature, @NotNull DataCollection.Version version) {
        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            return null;
        }

        DataCollection collection = getDataCollection(collectionName);
        return upsertDataCollectionTable(null, collection, table, role, version, signature);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataCollectionTable upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role,
                                                       DataCollection.Version version) {
        Table table = tableEntityMgr.findByName(tableName);
        if (table != null) {
            DataCollection collection = getDataCollection(collectionName);
            removeTableFromCollection(collectionName, tableName, version);
            return upsertDataCollectionTable(null, collection, table, role, version, null);
        }
        return null;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataCollectionTable upsertTableToCollection(@NotNull String collectionName, @NotNull String tableName,
            @NotNull TableRoleInCollection role, @NotNull String signature, @NotNull DataCollection.Version version) {
        Preconditions.checkNotNull(signature, "Signature should not be null");
        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            return null;
        }
        DataCollection collection = getDataCollection(collectionName);
        DataCollectionTable dcTable = dataCollectionTableReaderRepository.findFirstBySignature(collectionName, role,
                version, signature);
        return upsertDataCollectionTable(dcTable, collection, table, role, version, signature);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void removeTableFromCollection(String collectionName, String tableName, DataCollection.Version version) {
        log.info("Unlinking table {} in version {} in collection {}", tableName, version, collectionName);
        List<DataCollectionTable> dataCollectionTables = dataCollectionTableDao.findAllByName(collectionName, tableName,
                version);
        if (CollectionUtils.isNotEmpty(dataCollectionTables)) {
            dataCollectionTables.forEach(dataCollectionTableDao::delete);
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void removeAllTablesFromCollection(String customerSpace, DataCollection.Version version) {
        log.info("Unlinking all tables in version {} under tenant {}", version, customerSpace);
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        List<DataCollectionTable> dataCollectionTables = dataCollectionTableDao.findAllByFields(
                "FK_TENANT_ID", tenant.getPid(),
                "VERSION", version.toString()
        );
        if (CollectionUtils.isNotEmpty(dataCollectionTables)) {
            dataCollectionTables.forEach(dataCollectionTableDao::delete);
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void upsertStatsForMasterSegment(String collectionName, StatisticsContainer statisticsContainer) {
        DataCollection dataCollection = getDataCollection(collectionName);
        if (dataCollection == null) {
            throw new IllegalArgumentException("Cannot find data collection named " + collectionName);
        }
        MetadataSegment masterSeg = segmentEntityMgr.findMasterSegment(dataCollection.getName());
        if (masterSeg == null) {
            log.info("Did not see the master segment. Creating one now.");
            masterSeg = masterSegment(dataCollection);
            segmentEntityMgr.create(masterSeg);
        }
        segmentEntityMgr.upsertStats(masterSeg.getName(), statisticsContainer);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED, readOnly = true, noRollbackFor = {RuntimeException.class})
    @Override
    public DataCollection findDefaultCollection() {
        List<DataCollection> collections = dataCollectionDao.findAll();
        if (CollectionUtils.size(collections) != 1) {
            log.error("There are " + CollectionUtils.size(collections) + " data collections in current tenant. " +
                    "Cannot determine which one is the default.");
            throw new RuntimeException("There are " + CollectionUtils.size(collections) + " data collections in current tenant. "
                    + "Cannot determine which one is the default.");
        } else {
            return collections.get(0);
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection.Version findActiveVersion() {
        return findDefaultCollection().getVersion();
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection.Version findInactiveVersion() {
        DataCollection.Version activeVersion = findActiveVersion();
        return activeVersion.complement();
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataCollection createDefaultCollection() {
        DataCollection dataCollection = new DataCollection();
        return createDataCollection(dataCollection);
    }

    private DataCollection createDataCollection(DataCollection dataCollection) {
        if (StringUtils.isBlank(dataCollection.getName())) {
            dataCollection.setName(NamingUtils.timestamp("DC"));
        }
        if (getDataCollection(dataCollection.getName()) != null) {
            throw new IllegalStateException("Data collection " + dataCollection.getName() + " already exist.");
        }
        if (dataCollection.getVersion() == null) {
            dataCollection.setVersion(Blue);
        }
        dataCollection.setTenant(MultiTenantContext.getTenant());
        dataCollectionDao.create(dataCollection);
        if (segmentEntityMgr.findMasterSegment(dataCollection.getName()) == null) {
            // create master segment
            MetadataSegment segment = masterSegment(dataCollection);
            segmentEntityMgr.createOrUpdate(segment);
        }
        // create default feed
        DataFeed dataFeed = defaultFeed(dataCollection);
        dataFeedEntityMgr.createOrUpdate(dataFeed);
        return getDataCollection(dataCollection.getName());
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<DataCollectionTable> findTablesFromCollection(String collectionName, String tableName) {
        List<DataCollectionTable> list = dataCollectionTableDao.findAllByName(collectionName, tableName, null);
        if (CollectionUtils.isEmpty(list)) {
            return Collections.emptyList();
        } else {
            return list;
        }
    }

    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollectionTable findDataCollectionTableByPid(Long dataCollectionTablePid) {
        DataCollectionTable dataCollectionTable = dataCollectionTableReaderRepository.findByPid(dataCollectionTablePid);
        if (dataCollectionTable != null && dataCollectionTable.getTable() != null) {
            TableEntityMgr.inflateTable(dataCollectionTable.getTable());
        }
        return dataCollectionTable;
    }

    /*
     * Create or update input DataCollectionTable (can be null)
     */
    private DataCollectionTable upsertDataCollectionTable(DataCollectionTable dcTable,
            @NotNull DataCollection collection, @NotNull Table table, @NotNull TableRoleInCollection role,
            @NotNull DataCollection.Version version, String signature) {
        boolean createOnly = dcTable == null;
        if (dcTable == null) {
            dcTable = new DataCollectionTable();
        }
        dcTable.setTenant(MultiTenantContext.getTenant());
        dcTable.setDataCollection(collection);
        dcTable.setTable(table);
        dcTable.setRole(role);
        dcTable.setVersion(version);
        if (StringUtils.isNotEmpty(signature)) {
            dcTable.setSignature(signature);
        }

        if (createOnly) {
            dataCollectionTableDao.create(dcTable);
        } else {
            dataCollectionTableDao.createOrUpdate(dcTable);
        }
        return dcTable;
    }
}
