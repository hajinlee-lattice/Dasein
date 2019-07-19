package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.SegmentDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitable;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.apps.cdl.util.SegmentDependencyUtil;
import com.latticeengines.apps.core.annotation.SoftDeleteConfiguration;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegment> //
        implements SegmentEntityMgr, GraphVisitable {
    private static final Logger log = LoggerFactory.getLogger(SegmentEntityMgrImpl.class);

    @Inject
    private SegmentDao segmentDao;

    @Inject
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private RatingAttributeNameParser ratingAttributeNameParser;

    @Inject
    private SegmentDependencyUtil segmentDependencyUtil;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @SoftDeleteConfiguration
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
    public void delete(MetadataSegment segment, Boolean ignoreDependencyCheck, Boolean hardDelete) {
        if (Boolean.TRUE.equals(segment.getMasterSegment())) {
            throw new IllegalArgumentException("Cannot delete master segment");
        }
        segmentDao.update(segment);
        segmentDao.deleteByName(segment.getName(), hardDelete);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void revertDelete(String segmentName) {
        segmentDao.revertDeleteByName(segmentName);
    }

    @SoftDeleteConfiguration
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public List<String> getAllDeletedSegments() {
        return segmentDao.getAllDeletedSegments();
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public MetadataSegment createSegment(MetadataSegment segment) {
        preprocessBeforeCreateOrUpdate(segment);
        populateNullsBeforeCreate(segment);

        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            throw new RuntimeException("Segment already exists");
        } else {
            segmentDao.create(segment);
            return segment;
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment updateSegment(MetadataSegment segment, MetadataSegment existingSegment) {
        preprocessBeforeCreateOrUpdate(segment);

        if (existingSegment != null) {
            existingSegment = findByName(existingSegment.getName());
            existingSegment = cloneForUpdate(existingSegment, segment);
            segmentDao.update(existingSegment);
            setMetadataSegmentActionContext(existingSegment);
            return existingSegment;
        } else {
            throw new RuntimeException("Segment does not already exists");
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment updateSegmentWithoutAction(MetadataSegment segment, MetadataSegment existingSegment) {
        preprocessBeforeCreateOrUpdate(segment);

        if (existingSegment != null) {
            existingSegment = findByName(existingSegment.getName());
            existingSegment = cloneForUpdate(existingSegment, segment);
            segmentDao.update(existingSegment);
            return existingSegment;
        } else {
            throw new RuntimeException("Segment does not already exists");
        }
    }

    private void populateNullsBeforeCreate(MetadataSegment segment) {
        if (segment.getAccounts() == null) {
            segment.setAccounts(0L);
        }
        if (segment.getContacts() == null) {
            segment.setContacts(0L);
        }
    }

    private void preprocessBeforeCreateOrUpdate(MetadataSegment segment) {
        segment.setTenant(MultiTenantContext.getTenant());
        if (segment.getDataCollection() == null) {
            DataCollection defaultCollection = dataCollectionEntityMgr.findDefaultCollection();
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
    }

    private void setMetadataSegmentActionContext(MetadataSegment metadataSegment) {
        log.info(String.format("Set MetadataSegment Action Context for Segment %s", metadataSegment.getName()));
        Action metadataSegmentAction = new Action();
        metadataSegmentAction.setType(ActionType.METADATA_SEGMENT_CHANGE);
        metadataSegmentAction.setActionInitiator(metadataSegment.getCreatedBy());
        SegmentActionConfiguration configuration = new SegmentActionConfiguration();
        configuration.setSegmentName(metadataSegment.getName());
        metadataSegmentAction.setActionConfiguration(configuration);
        ActionContext.setAction(metadataSegmentAction);
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

    @Override
    public Set<Triple<String, String, String>> extractDependencies(MetadataSegment segment) {
        Set<Triple<String, String, String>> attrDepSet = new HashSet<Triple<String, String, String>>();
        if (segment != null) {
            segmentDependencyUtil.findSegmentDependingAttributes(segment);
            Set<AttributeLookup> attributeLookups = segment.getSegmentAttributes();
            for (AttributeLookup attributeLookup : attributeLookups) {
                if (attributeLookup.getEntity() == BusinessEntity.Rating) {
                    Pair<String, String> pair = ratingAttributeNameParser
                            .parseToTypeNModelId(attributeLookup.getAttribute());
                    attrDepSet.add(
                            ParsedDependencies.tuple(attributeLookup.getEntity() + "." + attributeLookup.getAttribute(), //
                                    pair.getLeft(), EdgeType.DEPENDS_ON));
                }
            }
        }
        if (CollectionUtils.isNotEmpty(attrDepSet)) {
            log.info(String.format("Extracted dependencies from segment %s: %s", segment.getName(),
                    JsonUtils.serialize(attrDepSet)));
        }
        return attrDepSet;
    }

    private MetadataSegment cloneForUpdate(MetadataSegment existing, MetadataSegment incoming) {
        existing.setAccounts(incoming.getAccounts());
        existing.setContacts(incoming.getContacts());
        existing.setProducts(incoming.getProducts());
        existing.setDisplayName(incoming.getDisplayName());
        existing.setDescription(incoming.getDescription());
        existing.setUpdatedBy(incoming.getUpdatedBy());
        if (!Boolean.TRUE.equals(existing.getMasterSegment())) {
            existing.setAccountRestriction(incoming.getAccountRestriction());
            existing.setContactRestriction(incoming.getContactRestriction());
        }
        return existing;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void accept(GraphVisitor visitor, Object entity) throws Exception {
        visitor.visit((MetadataSegment) entity, parse((MetadataSegment) entity, null));
    }
}
