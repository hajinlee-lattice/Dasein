package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.DataCollection.Version.Blue;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataCollectionDao;
import com.latticeengines.apps.cdl.dao.DataCollectionTableDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.repository.reader.DataCollectionTableReaderRepository;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
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
            String collectionName, TableRoleInCollection tableRole, DataCollection.Version version){
        return dataCollectionDao.findTableNamesOfAllRole(collectionName,tableRole,version);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role,
            DataCollection.Version version) {
        Table table = tableEntityMgr.findByName(tableName);
        if (table != null) {
            DataCollection collection = getDataCollection(collectionName);
            removeTableFromCollection(collectionName, tableName, version);
            DataCollectionTable dataCollectionTable = new DataCollectionTable();
            dataCollectionTable.setTenant(MultiTenantContext.getTenant());
            dataCollectionTable.setDataCollection(collection);
            dataCollectionTable.setTable(table);
            dataCollectionTable.setRole(role);
            dataCollectionTable.setVersion(version);
            dataCollectionTableDao.create(dataCollectionTable);
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void removeTableFromCollection(String collectionName, String tableName, DataCollection.Version version) {
        List<DataCollectionTable> dataCollectionTables = dataCollectionTableDao.findAllByName(collectionName, tableName,
                version);
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
}
