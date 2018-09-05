package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

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
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
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
        MetadataSegment segment1 = null;

        if (segment.getName() != null) {
            MetadataSegment existingSegment = findByName(segment.getName());
            if (existingSegment != null) {
                segment1 = segmentEntityMgr.updateSegment(segment, existingSegment);
                evictRatingMetadataCache();
                return segment1;
            }
        }

        if (StringUtils.isBlank(segment.getName())) {
            segment.setName(NamingUtils.timestamp("Segment"));
        }
        return segmentEntityMgr.createSegment(segment);
    }

    @Override
    public Boolean deleteSegmentByName(String segmentName, boolean ignoreDependencyCheck) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName);
        if (segment == null) {
            return false;
        }

        segmentEntityMgr.delete(segment, ignoreDependencyCheck);
        return true;
    }

    @Override
    public List<MetadataSegment> getSegments() {
        String collectionName = dataCollectionEntityMgr.findDefaultCollection().getName();
        return segmentEntityMgr.findAllInCollection(collectionName);
    }

    @Override
    @NoCustomerSpace
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
    @NoCustomerSpace
    public Map<BusinessEntity, Long> updateSegmentCounts(String segmentName) {
        Map<BusinessEntity, Long> map = new HashMap<>();
        MetadataSegment existingSegment = findByName(segmentName);
        if (existingSegment != null) {
            // use a deep copy to avoid changing restriction format to break UI
            MetadataSegment segmentCopy = JsonUtils.deserialize(JsonUtils.serialize(existingSegment),
                    MetadataSegment.class);
            Map<BusinessEntity, Long> counts = getEntityCounts(segmentCopy);
            counts.forEach(segmentCopy::setEntityCount);

            log.info("Updating counts for segment " + segmentName + " to "
                    + JsonUtils.serialize(segmentCopy.getEntityCounts()));
            existingSegment = segmentEntityMgr.updateSegment(segmentCopy, existingSegment);
            evictRatingMetadataCache();

            map = existingSegment.getEntityCounts();
        }
        return map;
    }

    @NoCustomerSpace
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

    @NoCustomerSpace
    private Long getEntityCount(BusinessEntity entity, FrontEndRestriction accountRestriction,
            FrontEndRestriction contactRestriction) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        if (accountRestriction != null) {
            frontEndQuery.setAccountRestriction(accountRestriction);
        }
        if (contactRestriction != null) {
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

    private void evictRatingMetadataCache() {
        String tenantId = MultiTenantContext.getShortTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.DataCloudCMCache);
    }

    @Override
    @NoCustomerSpace
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

    @NoCustomerSpace
    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }
}
