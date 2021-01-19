package com.latticeengines.apps.cdl.entitymgr.impl;

import static com.latticeengines.domain.exposed.metadata.MetadataSegment.SegmentType;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.SegmentDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitable;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.apps.cdl.entitymgr.ListSegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.apps.core.annotation.SoftDeleteConfiguration;
import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.SegmentDependencyUtil;
import com.latticeengines.domain.exposed.util.SegmentUtils;

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
    private ListSegmentEntityMgr listSegmentEntityMgr;

    @Inject
    private SegmentEntityMgr _self;

    private Random random = new Random();

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

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MetadataSegment> findByType(SegmentType type) {
        return segmentDao.findAllByField("type", type);
    }

    @SoftDeleteConfiguration
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String name, boolean inflate) {
        MetadataSegment segment = findByName(name);
        if (segment != null && inflate) {
            Hibernate.initialize(segment.getListSegment());
        }
        return segment;
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
        if (hardDelete) {
            segmentDao.delete(segment);
        } else {
            segmentDao.deleteByName(segment.getName(), false);
            if (SegmentUtils.hasListSegment(segment)) {
                ListSegment listSegment = segment.getListSegment();
                listSegmentEntityMgr.delete(listSegment);
            }
        }
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

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment createListSegment(MetadataSegment segment) {
        segment.setType(SegmentType.List);
        preprocessBeforeCreateOrUpdate(segment);
        populateNullsBeforeCreate(segment);
        segmentDao.create(segment);
        ListSegment listSegment = segment.getListSegment();
        if (listSegment != null) {
            listSegment.setTenantId(MultiTenantContext.getTenant().getPid());
            listSegment.setMetadataSegment(segment);
            listSegmentEntityMgr.create(listSegment);
        }
        return segment;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment createSegment(MetadataSegment segment) {
        segment.setType(SegmentType.Query);
        preprocessBeforeCreateOrUpdate(segment);
        populateNullsBeforeCreate(segment);
        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            throw new RuntimeException("Segment already exists");
        } else {
            segmentDao.create(segment);
        }
        return segment;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment updateSegment(MetadataSegment segment, MetadataSegment existingSegment) {
        log.info("Updating segment {} with action and auditing", segment.getName());
        preprocessBeforeCreateOrUpdate(segment);
        if (existingSegment != null) {
            existingSegment = findByName(existingSegment.getName());
            cloneForUpdate(existingSegment, segment);
            existingSegment.setSkipAuditing(false);
            segmentDao.update(existingSegment);
            setMetadataSegmentActionContext(existingSegment);
            return existingSegment;
        } else {
            throw new RuntimeException("Segment does not exists");
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment updateListSegment(MetadataSegment incomingSegment, MetadataSegment existingSegment) {
        cloneForUpdate(existingSegment, incomingSegment);
        segmentDao.update(existingSegment);
        return existingSegment;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MetadataSegment findByExternalInfo(String externalSystem, String externalSegmentId) {
        MetadataSegment metadataSegment = segmentDao.findByExternalInfo(externalSystem, externalSegmentId);
        return metadataSegment;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MetadataSegment findByExternalInfo(MetadataSegment segment) {
        if (segment.getListSegment() != null) {
            return segmentDao.findByExternalInfo(segment.getListSegment().getExternalSystem(), segment.getListSegment().getExternalSegmentId());
        } else {
            return null;
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public MetadataSegment updateSegmentWithoutActionAndAuditing(MetadataSegment segment, MetadataSegment existingSegment) {
        log.info("Updating segment {} without action and auditing", existingSegment.getName());
        preprocessBeforeCreateOrUpdate(segment);
        if (existingSegment != null) {
            existingSegment = findByName(existingSegment.getName());
            cloneForUpdate(existingSegment, segment);
            existingSegment.setSkipAuditing(true);
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
        segment.setTeamId(StringUtils.isEmpty(segment.getTeamId()) ? TeamUtils.GLOBAL_TEAM_ID : segment.getTeamId());
        if (StringUtils.isEmpty(segment.getDisplayName())) {
            segment.setDisplayName(SegmentUtils.generateDisplayName(MultiTenantContext.getShortTenantId()));
        }
    }

    private void setDataCollection(MetadataSegment segment) {
        if (segment.getDataCollection() == null) {
            DataCollection defaultCollection = dataCollectionEntityMgr.findDefaultCollection();
            segment.setDataCollection(defaultCollection);
        }
    }

    private void setSegmentName(MetadataSegment segment) {
        if (StringUtils.isBlank(segment.getName())) {
            segment.setName(NamingUtils.timestamp("Segment"));
        }
    }

    private void preprocessBeforeCreateOrUpdate(MetadataSegment segment) {
        segment.setTenant(MultiTenantContext.getTenant());
        setDataCollection(segment);
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
        setSegmentName(segment);
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
            SegmentDependencyUtil.findSegmentDependingAttributes(segment);
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
        if (incoming.getAccounts() != null) {
            existing.setAccounts(incoming.getAccounts());
        }
        if (incoming.getContacts() != null) {
            existing.setContacts(incoming.getContacts());
        }
        if (StringUtils.isNotEmpty(incoming.getDisplayName())) {
            existing.setDisplayName(incoming.getDisplayName());
        }
        existing.setDescription(incoming.getDescription());
        if (StringUtils.isNotEmpty(incoming.getUpdatedBy())) {
            existing.setUpdatedBy(incoming.getUpdatedBy());
        }
        if (StringUtils.isNotEmpty(incoming.getTeamId())) {
            existing.setTeamId(incoming.getTeamId());
        }
        if (incoming.getCountsOutdated() != null) {
            existing.setCountsOutdated(incoming.getCountsOutdated());
        }
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
