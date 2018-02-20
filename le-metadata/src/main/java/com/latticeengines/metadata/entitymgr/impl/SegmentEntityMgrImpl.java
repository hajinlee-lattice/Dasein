package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.metadata.dao.SegmentDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegment> implements SegmentEntityMgr {
    private static final Logger log = LoggerFactory.getLogger(SegmentEntityMgrImpl.class);

    @Autowired
    private SegmentDao segmentDao;

    @Autowired
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String name) {
        return segmentDao.findByField("name", name);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<MetadataSegment> findAll() {
        return super.findAll().stream().collect(Collectors.toList());
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void delete(MetadataSegment segment) {
        if (Boolean.TRUE.equals(segment.getMasterSegment())) {
            throw new IllegalArgumentException("Cannot delete master segment");
        }
        segmentDao.update(segment);
        segmentDao.delete(segment);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(MetadataSegment segment) {
        segment.setTenant(MultiTenantContext.getTenant());
        if (segment.getDataCollection() == null) {
            DataCollection defaultCollection = dataCollectionEntityMgr.getOrCreateDefaultCollection();
            segment.setDataCollection(defaultCollection);
        }
        MetadataSegment master = findMasterSegment(segment.getDataCollection().getName());
        if (Boolean.TRUE.equals(segment.getMasterSegment())) {
            if (master != null && !master.getName().equals(segment.getName())) {
                // master exists and not the incoming one
                segment.setMasterSegment(false);
            }
        } else {
            if (master != null && master.getName().equals(segment.getName())) {
                throw new IllegalArgumentException(
                        "Segment " + segment.getName() + " is the master segment, cannot change it to non-master.");
            }
        }
        if (StringUtils.isBlank(segment.getName())) {
            segment.setName(NamingUtils.timestamp("Segment"));
        }

        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            existing = cloneForUpdate(existing, segment);
            segmentDao.update(existing);
        } else {
            segmentDao.create(segment);
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<MetadataSegment> findAllInCollection(String collectionName) {
        return super.findAll().stream() //
                .filter(s -> s.getDataCollection().getName().equals(collectionName)) //
                .collect(Collectors.toList());
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findMasterSegment(String collectionName) {
        return segmentDao.findMasterSegment(collectionName);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void upsertStats(String segmentName, StatisticsContainer statisticsContainer) {
        MetadataSegment segment = findByName(segmentName);
        if (segment == null) {
            throw new IllegalStateException("Cannot find segment named " + segmentName + " in database");
        }
        DataCollection.Version version = statisticsContainer.getVersion();
        if (version == null) {
            throw new IllegalArgumentException("Must set data collection version for new statistics container.");
        }
        StatisticsContainer oldStats = statisticsContainerEntityMgr.findInSegment(segmentName, version);
        if (oldStats != null) {
            log.info("There is already a main stats for segment " + segmentName + ". Remove it first.");
            statisticsContainerEntityMgr.delete(oldStats);
        }
        if (StringUtils.isBlank(statisticsContainer.getName())) {
            statisticsContainer.setName(NamingUtils.timestamp("Stats"));
        }
        statisticsContainer.setSegment(segment);
        statisticsContainer.setTenant(MultiTenantContext.getTenant());
        statisticsContainerEntityMgr.create(statisticsContainer);
    }

    private MetadataSegment cloneForUpdate(MetadataSegment existing, MetadataSegment incoming) {
        existing.setAccounts(incoming.getAccounts());
        existing.setContacts(incoming.getContacts());
        existing.setProducts(incoming.getProducts());
        existing.setDisplayName(incoming.getDisplayName());
        existing.setDescription(incoming.getDescription());
        if (!Boolean.TRUE.equals(existing.getMasterSegment())) {
            existing.setAccountRestriction(incoming.getAccountRestriction());
            existing.setContactRestriction(incoming.getContactRestriction());
        }
        return existing;
    }

}
