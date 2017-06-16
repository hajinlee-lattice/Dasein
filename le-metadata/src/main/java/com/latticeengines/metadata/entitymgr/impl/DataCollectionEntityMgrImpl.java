package com.latticeengines.metadata.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.MetadataConstants.DATE_FORMAT;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.metadata.dao.DataCollectionDao;
import com.latticeengines.metadata.dao.DataCollectionPropertyDao;
import com.latticeengines.metadata.dao.DataCollectionTableDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.SegmentationDataCollectionService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataCollectionEntityMgr")
public class DataCollectionEntityMgrImpl extends BaseEntityMgrImpl<DataCollection> implements DataCollectionEntityMgr {

    private static final Log log = LogFactory.getLog(DataCollectionEntityMgrImpl.class);

    @Autowired
    private DataCollectionDao dataCollectionDao;

    @Autowired
    private DataCollectionTableDao dataCollectionTableDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private DataCollectionPropertyDao dataCollectionPropertyDao;

    @Autowired
    private SegmentationDataCollectionService segmentationDataCollectionService;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Override
    public DataCollectionDao getDao() {
        return dataCollectionDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createDataCollection(DataCollection dataCollection) {
//        removeDefaultTables(dataCollection);
        if (getDataCollection(dataCollection.getName()) != null) {
            throw new IllegalStateException("Data collection " + dataCollection.getName() + " already exist.");
        }
        dataCollection.setTenant(MultiTenantContext.getTenant());
        if (StringUtils.isBlank(dataCollection.getName())) {
            dataCollection.setName("DataCollection_" + DATE_FORMAT.format(new Date()));
        }
        create(dataCollection);
        for (DataCollectionProperty dataCollectionProperty : dataCollection.getProperties()) {
            dataCollectionProperty.setDataCollection(dataCollection);
            dataCollectionPropertyDao.create(dataCollectionProperty);
        }
        if (segmentEntityMgr.findMasterSegment(dataCollection.getName()) == null) {
            // create master segment
            MetadataSegment segment = masterSegment(dataCollection);
            segmentEntityMgr.createOrUpdate(segment);
        }
    }

    private MetadataSegment masterSegment(DataCollection dataCollection) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDataCollection(dataCollection);
        segment.setName("Segment_" + UUID.randomUUID().toString().replace("-", ""));
        segment.setDisplayName("Customer Universe");
        segment.setDescription("Master segment of the collection " + dataCollection.getName());
        segment.setUpdated(new Date());
        segment.setCreated(new Date());
        segment.setMasterSegment(true);
        return segment;
    }

    private void removeDefaultTables(DataCollection dataCollection) {
        if (dataCollection.getType() == DataCollectionType.Segmentation) {
            segmentationDataCollectionService.removeDefaultTables(dataCollection);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public DataCollection getDataCollection(String name) {
        List<DataCollection> candidates = dataCollectionDao.findAllByField("name", name);
        if (candidates.size() == 0) {
            return null;
        }
        DataCollection dataCollection = candidates.get(0);
        HibernateUtils.inflateDetails(dataCollection.getProperties());
        return dataCollection;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public DataCollection getDataCollection(DataCollectionType type) {
        DataCollection collection = findByField("type", type);
        if (collection != null) {
            HibernateUtils.inflateDetails(collection.getProperties());
        } else {
//            if (registerDefault(type)) {
//                collection = getDataCollection(type);
//            }
        }

        return collection;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public void fillInTables(DataCollection dataCollection) {
        List<Table> tables = getTablesOfRole(dataCollection.getName(), null);
        dataCollection.setTables(tables);
        fillInDefaultTables(dataCollection);
    }

    private void fillInDefaultTables(DataCollection dataCollection) {
        if (dataCollection.getType() == DataCollectionType.Segmentation) {
            segmentationDataCollectionService.fillInDefaultTables(dataCollection);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeDataCollection(String name) {
        DataCollection dataCollection = getDataCollection(name);

        List<Table> tablesInCollection = getTablesOfRole(name, null);
        tablesInCollection.forEach(t -> removeTableFromCollection(name, t.getName()));

        dataCollectionDao.delete(dataCollection);
    }

    private boolean registerDefault(DataCollectionType type) {
        if (type == DataCollectionType.Segmentation) {
            DataCollection collection = segmentationDataCollectionService.getDefaultDataCollection();
            createDataCollection(collection);
            return true;
        }

        return false;
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public List<Table> getTablesOfRole(String collectionName, TableRoleInCollection tableRole) {
        List<String> tableNames = dataCollectionDao.getTableNamesOfRole(collectionName, tableRole);
        if (tableNames == null) {
            return Collections.emptyList();
        }
        return tableNames.stream().map(tableEntityMgr::findByName).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role) {
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
            dataCollectionTableDao.create(dataCollectionTable);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeTableFromCollection(String collectionName, String tableName) {
        DataCollectionTable dataCollectionTable = dataCollectionTableDao.findByNames(collectionName, tableName);
        if (dataCollectionTable != null) {
            dataCollectionTableDao.create(dataCollectionTable);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void upsertStatsForMasterSegment(String collectionName, StatisticsContainer statisticsContainer,
            String modelId) {
        DataCollection dataCollection = getDataCollection(collectionName);
        if (dataCollection == null) {
            throw new IllegalArgumentException("Cannot find data collection named " + collectionName);
        }
        MetadataSegment masterSeg = segmentEntityMgr.findMasterSegment(dataCollection.getName());
        if (masterSeg == null) {
            log.info("Did not see the master segment. Creating one now.");
            masterSeg = masterSegment(dataCollection);
            segmentEntityMgr.create(masterSeg);
            masterSeg = segmentEntityMgr.findMasterSegment(dataCollection.getName());
        }
        if (masterSeg == null) {
            throw new IllegalStateException("Cannot find master segment of the collection " + collectionName);
        }
        StatisticsContainer oldStats = statisticsContainerEntityMgr.findInMasterSegment(collectionName, modelId);
        if (oldStats != null) {
            log.info("There is already a main stats for collection " + collectionName + ". Remove it first.");
            statisticsContainerEntityMgr.delete(oldStats);
        }
        statisticsContainer.setSegment(masterSeg);
        statisticsContainerEntityMgr.create(statisticsContainer);
    }

}
