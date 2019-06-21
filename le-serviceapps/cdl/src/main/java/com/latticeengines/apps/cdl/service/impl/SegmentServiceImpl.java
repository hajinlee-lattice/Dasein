package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.impl.DependencyChecker;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.util.SegmentDependencyUtil;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {

    private static final Logger log = LoggerFactory.getLogger(SegmentServiceImpl.class);

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private StatisticsContainerEntityMgr statisticsContainerEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private SegmentDependencyUtil segmentDependencyUtil;

    @Inject
    private DependencyChecker dependencyChecker;

    @Override
    public MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        validateSegment(segment);
        MetadataSegment persistedSegment;
        if (StringUtils.isNotBlank(segment.getName())) {
            MetadataSegment existingSegment = findByName(segment.getName());
            if (existingSegment != null) {
                persistedSegment = segmentEntityMgr.updateSegment(segment, existingSegment);
                evictRatingMetadataCache(existingSegment, segment);
            } else {
                persistedSegment = segmentEntityMgr.createSegment(segment);
            }
        } else {
            segment.setName(NamingUtils.timestamp("Segment"));
            persistedSegment = segmentEntityMgr.createSegment(segment);
        }

        if (persistedSegment != null) {
            updateSegmentCounts(persistedSegment);
        }
        return persistedSegment;
    }

    @Override
    public Boolean deleteSegmentByName(String segmentName, boolean ignoreDependencyCheck,
            boolean hardDelete) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName);
        if (segment == null) {
            return false;
        }
        segmentEntityMgr.delete(segment, ignoreDependencyCheck, hardDelete);
        return true;
    }

    @Override
    public Boolean revertDeleteSegmentByName(String segmentName) {
        segmentEntityMgr.revertDelete(segmentName);
        return true;
    }

    @Override
    public List<String> getAllDeletedSegments() {
        return segmentEntityMgr.getAllDeletedSegments();
    }

    @Override
    public List<MetadataSegment> getSegments() {
        String collectionName = dataCollectionEntityMgr.findDefaultCollection().getName();
        return segmentEntityMgr.findAllInCollection(collectionName);
    }

    @Override
    public MetadataSegment findByName(String name) {
        return segmentEntityMgr.findByName(name);
    }

    @Override
    public MetadataSegment findMaster(String collectionName) {
        return segmentEntityMgr.findMasterSegment(collectionName);
    }

    @Override
    public StatisticsContainer getStats(String segmentName, DataCollection.Version version) {
        if (version == null) {
            // by default get from active version
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        return statisticsContainerEntityMgr.findInSegment(segmentName, version);
    }

    @Override
    public void upsertStats(String segmentName, StatisticsContainer statisticsContainer) {
        segmentEntityMgr.upsertStats(segmentName, statisticsContainer);
    }

    @Override
    public Map<BusinessEntity, Long> updateSegmentCounts(String segmentName) {
        Map<BusinessEntity, Long> map = new HashMap<>();
        MetadataSegment existingSegment = findByName(segmentName);
        if (existingSegment != null) {
            map = updateSegmentCounts(existingSegment);
        }
        return map;
    }

    @Override
    public Map<String, Map<BusinessEntity, Long>> updateSegmentsCounts() {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<MetadataSegment> segments = getSegments();
            log.info("Updating counts for " + CollectionUtils.size(segments) + " segments.");
            Map<String, Map<BusinessEntity, Long>> review = new HashMap<>();
            if (CollectionUtils.isNotEmpty(segments)) {
                // Do not parallel, as it will be bottle-necked at objectapi
                segments.forEach(segment -> {
                    String name = segment.getName();
                    try {
                        Map<BusinessEntity, Long> counts = updateSegmentCounts(segment);
                        review.put(name, counts);
                    } catch (Exception e) {
                        log.warn("Failed to update counts for segment " + name + //
                        " in tenant " + MultiTenantContext.getShortTenantId());
                    }
                });
            }
            timer.setTimerMessage("Finished updating counts for " + CollectionUtils.size(segments) + " segments.");
            return review;
        }
    }

    private Map<BusinessEntity, Long> getEntityCounts(MetadataSegment segment) {
        Map<BusinessEntity, Long> map = new HashMap<>();
        FrontEndRestriction accountRestriction = segment.getAccountFrontEndRestriction();
        if (accountRestriction == null) {
            accountRestriction = new FrontEndRestriction(segment.getAccountRestriction());
        }
        FrontEndRestriction contactRestriction = segment.getContactFrontEndRestriction();
        if (contactRestriction == null) {
            contactRestriction = new FrontEndRestriction(segment.getContactRestriction());
        }
        for (BusinessEntity entity : BusinessEntity.COUNT_ENTITIES) {
            try {
                Long count = getEntityCount(entity, accountRestriction, contactRestriction);
                if (count != null) {
                    map.put(entity, count);
                }
            } catch (Exception e) {
                log.warn("Failed to count " + entity + ": " + e.getMessage());
            }
        }
        return map;
    }

    private Map<BusinessEntity, Long> updateSegmentCounts(MetadataSegment segment) {
        // use a deep copy to avoid changing restriction format to break UI
        MetadataSegment segmentCopy = JsonUtils.deserialize(JsonUtils.serialize(segment), MetadataSegment.class);
        Map<BusinessEntity, Long> counts = getEntityCounts(segmentCopy);
        counts.forEach(segmentCopy::setEntityCount);
        log.info("Updating counts for segment " + segment.getName() + " (" + segment.getDisplayName() + ")" //
                + " to " + JsonUtils.serialize(segmentCopy.getEntityCounts()));
        segment = segmentEntityMgr.updateSegmentWithoutAction(segmentCopy, segment);
        return segment.getEntityCounts();
    }

    private Long getEntityCount(BusinessEntity entity, FrontEndRestriction accountRestriction,
            FrontEndRestriction contactRestriction) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        Restriction accountExists = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()).isNotNull().build();
        if (accountRestriction != null && accountRestriction.getRestriction() != null) {
            if (BusinessEntity.Contact.equals(entity)) {
                Restriction combined = Restriction.builder() //
                        .and(accountRestriction.getRestriction(), accountExists).build();
                frontEndQuery.setAccountRestriction(new FrontEndRestriction(combined));
            } else {
                frontEndQuery.setAccountRestriction(accountRestriction);
            }
        } else if (BusinessEntity.Contact.equals(entity)) {
            frontEndQuery.setAccountRestriction(new FrontEndRestriction(accountExists));
        }
        if (contactRestriction != null && contactRestriction.getRestriction() != null) {
            frontEndQuery.setContactRestriction(contactRestriction);
        }
        frontEndQuery.setMainEntity(entity);
        return entityProxy.getCount(customerSpace, frontEndQuery);
    }

    @Override
    public Map<String, List<String>> getDependencies(String segmentName) throws Exception {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return dependencyChecker.getDependencies(customerSpace, segmentName, CDLObjectTypes.Segment.name());
    }

    // Only when a segment displayName got changed, then we need to evict this
    // DataLakeCache
    private void evictRatingMetadataCache(MetadataSegment existingSegment, MetadataSegment updatedSegment) {
        if (existingSegment == null || updatedSegment == null) {
            return;
        }
        if (StringUtils.equalsAny(existingSegment.getDisplayName(), updatedSegment.getDisplayName())) {
            return;
        }
        String tenantId = MultiTenantContext.getShortTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.DataLakeCMCache);
    }

    @Override
    public List<AttributeLookup> findDependingAttributes(List<MetadataSegment> metadataSegments) {
        Set<AttributeLookup> dependingAttributes = new HashSet<>();
        if (metadataSegments != null) {
            for (MetadataSegment metadataSegment : metadataSegments) {
                segmentDependencyUtil.findSegmentDependingAttributes(metadataSegment);
                Set<AttributeLookup> attributeLookups = metadataSegment.getSegmentAttributes();
                if (attributeLookups != null) {
                    dependingAttributes.addAll(metadataSegment.getSegmentAttributes());
                }
            }
        }

        return new ArrayList<>(dependingAttributes);
    }

    @Override
    public List<MetadataSegment> findDependingSegments(List<String> attributes) {
        List<MetadataSegment> dependingMetadataSegments = new ArrayList<>();
        if (attributes != null) {
            List<MetadataSegment> metadataSegments = getSegments();
            if (metadataSegments != null) {
                for (MetadataSegment metadataSegment : metadataSegments) {
                    segmentDependencyUtil.findSegmentDependingAttributes(metadataSegment);
                    Set<AttributeLookup> segmentAttributes = metadataSegment.getSegmentAttributes();
                    if (segmentAttributes != null) {
                        for (AttributeLookup attributeLookup : segmentAttributes) {
                            if (attributes.contains(sanitize(attributeLookup.toString()))) {
                                dependingMetadataSegments.add(metadataSegment);
                                break;
                            }
                        }
                    }
                }
            }
        }

        return dependingMetadataSegments;
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }

    private void validateSegment(MetadataSegment segment) {
        List<BucketRestriction> invalidBkts = new ArrayList<>();
        try {
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(segment.getAccountRestriction()));
            invalidBkts.addAll(RestrictionUtils.validateBktsInRestriction(segment.getContactRestriction()));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_40057, e, new String[]{ e.getMessage() });
        }
        if (CollectionUtils.isNotEmpty(invalidBkts)) {
            String message = invalidBkts.stream() //
                    .map(BucketRestriction::getAttr) //
                    .map(AttributeLookup::toString) //
                    .collect(Collectors.joining(","));
            throw new LedpException(LedpCode.LEDP_40057, new String[]{ message });
        }
    }
}
