package com.latticeengines.metadata.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.DataCollection.Version.Blue;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.metadata.dao.DataCollectionDao;
import com.latticeengines.metadata.dao.DataCollectionTableDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataCollectionEntityMgr")
public class DataCollectionEntityMgrImpl extends BaseEntityMgrImpl<DataCollection> implements DataCollectionEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionEntityMgrImpl.class);

    @Autowired
    private DataCollectionDao dataCollectionDao;

    @Autowired
    private DataCollectionTableDao dataCollectionTableDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private DataFeedEntityMgr dataFeedEntityMgr;

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

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection getDataCollection(String name) {
        return dataCollectionDao.findByField("name", name);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeDataCollection(String name) {
        DataCollection dataCollection = getDataCollection(name);

        List<Table> tablesInCollection = getTablesOfRole(name, null, null);
        tablesInCollection.forEach(t -> removeTableFromCollection(name, t.getName()));

        dataCollectionDao.delete(dataCollection);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<Table> getTablesOfRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version) {
        List<String> tableNames = dataCollectionDao.getTableNamesOfRole(collectionName, tableRole, version);
        if (tableNames == null) {
            return Collections.emptyList();
        }
        return tableNames.stream().map(tableEntityMgr::findByName).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<String> getTableNamesOfRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version) {
        List<String> tableNames = dataCollectionDao.getTableNamesOfRole(collectionName, tableRole, version);
        if (tableNames == null) {
            return Collections.emptyList();
        } else {
            return tableNames;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role, DataCollection.Version version) {
        Table table = tableEntityMgr.findByName(tableName);
        if (table != null) {
            DataCollection collection = getDataCollection(collectionName);
            DataCollectionTable dataCollectionTable = dataCollectionTableDao.findByNames(collectionName, tableName);
            if (dataCollectionTable == null) {
                dataCollectionTable = new DataCollectionTable();
                dataCollectionTable.setTenant(MultiTenantContext.getTenant());
            } else {
                dataCollectionTableDao.delete(dataCollectionTable);
            }
            dataCollectionTable.setDataCollection(collection);
            dataCollectionTable.setTable(table);
            dataCollectionTable.setRole(role);
            dataCollectionTable.setVersion(version);
            dataCollectionTableDao.create(dataCollectionTable);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeTableFromCollection(String collectionName, String tableName) {
        DataCollectionTable dataCollectionTable = dataCollectionTableDao.findByNames(collectionName, tableName);
        if (dataCollectionTable != null) {
            dataCollectionTableDao.delete(dataCollectionTable);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
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

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public DataCollection getOrCreateDefaultCollection() {
        List<DataCollection> collections = dataCollectionDao.findAll();
        if (collections == null || collections.isEmpty()) {
            return createDefaultCollection();
        }
        if (collections.size() == 1) {
            return collections.get(0);
        }
        throw new RuntimeException("There are " + collections.size() + " data collections in current tenant. "
                + "Cannot determine which one is the default.");
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection getDefaultCollectionReadOnly() {
        List<DataCollection> collections = dataCollectionDao.findAll();
        if (collections == null || collections.isEmpty()) {
            throw new RuntimeException("Default collection has not been created yet.");
        }
        if (collections.size() == 1) {
            return collections.get(0);
        }
        throw new RuntimeException("There are " + collections.size() + " data collections in current tenant. "
                + "Cannot determine which one is the default.");
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection.Version getActiveVersion() {
        return getDefaultCollectionReadOnly().getVersion();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection.Version getInactiveVersion() {
        DataCollection.Version activeVersion = getActiveVersion();
        return activeVersion.complement();
    }

    private DataCollection createDefaultCollection() {
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

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollectionTable getTableFromCollection(String collectionName, String tableName) {
        return dataCollectionTableDao.findByNames(collectionName, tableName);
    }
}
