package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StatisticsContainerEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.impl.DependencyChecker;
import com.latticeengines.apps.cdl.service.RatingEngineService;
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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.pls.RatingEngine;
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
    private RatingEngineService ratingEngineService;

    @Inject
    private SegmentDependencyUtil segmentDependencyUtil;

    @Inject
    private DependencyChecker dependencyChecker;

    @Override
    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment) {
        verifySegmentCyclicDependency(segment);
        MetadataSegment segment1 = null;

        if (segment.getName() != null) {
            MetadataSegment existingSegment = findByName(segment.getName());
            if (existingSegment != null) {
                segment1 = segmentEntityMgr.updateSegment(segment, existingSegment);
                evictRatingMetadataCache();
                return segment1;
            }
        }

        segment.setName(NamingUtils.timestamp("Segment"));
        return segmentEntityMgr.createSegment(segment);
    }

    @Override
    public Boolean deleteSegmentByName(String customerSpace, String segmentName, boolean ignoreDependencyCheck) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName);
        if (segment == null) {
            return false;
        }

        segmentEntityMgr.delete(segment, ignoreDependencyCheck);
        return true;
    }

    @Override
    public List<MetadataSegment> getSegments(String customerSpace) {
        String collectionName = dataCollectionEntityMgr.findDefaultCollection().getName();
        return segmentEntityMgr.findAllInCollection(collectionName);
    }

    @Override
    public List<MetadataSegment> getSegments(String customerSpace, String collectionName) {
        List<MetadataSegment> segments = segmentEntityMgr.findAll();
        if (segments == null || segments.isEmpty()) {
            return Collections.emptyList();
        }
        return segments.stream() //
                .filter(segment -> collectionName.equals(segment.getDataCollection().getName())) //
                .collect(Collectors.toList());
    }

    @Override
    @NoCustomerSpace
    public MetadataSegment findByName(String name) {
        return segmentEntityMgr.findByName(name);
    }

    @Override
    public MetadataSegment findByName(String customerSpace, String name) {
        return segmentEntityMgr.findByName(name);
    }

    @Override
    public MetadataSegment findMaster(String customerSpace, String collectionName) {
        return segmentEntityMgr.findMasterSegment(collectionName);
    }

    @Override
    public StatisticsContainer getStats(String customerSpace, String segmentName, DataCollection.Version version) {
        if (version == null) {
            // by default get from active version
            version = dataCollectionEntityMgr.findActiveVersion();
        }
        return statisticsContainerEntityMgr.findInSegment(segmentName, version);
    }

    @Override
    public void upsertStats(String customerSpace, String segmentName, StatisticsContainer statisticsContainer) {
        segmentEntityMgr.upsertStats(segmentName, statisticsContainer);
    }

    @Override
    public void deleteAllSegments(String customerSpace, boolean ignoreDependencyCheck) {
        List<MetadataSegment> segments = getSegments(customerSpace);
        for (MetadataSegment segment : segments) {
            deleteSegmentByName(customerSpace, segment.getName(), ignoreDependencyCheck);
        }
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
            segmentEntityMgr.updateSegment(segmentCopy, existingSegment);
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
    public Map<CDLObjectTypes, List<String>> getDependencies(String customerSpace, String segmentName)
            throws Exception {
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
    public List<MetadataSegment> findDependingSegments(String customerSpace, List<String> attributes) {
        List<MetadataSegment> dependingMetadataSegments = new ArrayList<>();
        if (attributes != null) {
            List<MetadataSegment> metadataSegments = getSegments(customerSpace);
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

    @Override
    public void verifySegmentCyclicDependency(MetadataSegment metadataSegment) {
        if (metadataSegment != null) {
            MetadataSegment existing = findByName(metadataSegment.getName());
            if (existing != null) {
                Map<Long, String> segmentMap = segmentCyclicDependency(existing, new LinkedHashMap<>());
                if (segmentMap != null) {
                    StringBuilder message = new StringBuilder();
                    for (Map.Entry<Long, String> entry : segmentMap.entrySet()) {
                        if (entry.getKey() != -1) {
                            message.append(String.format("Segment '%s' --> ", entry.getValue()));
                        } else {
                            message.append(String.format("Segment '%s'.", entry.getValue()));
                        }
                    }

                    throw new LedpException(LedpCode.LEDP_40025, new String[] { message.toString() });
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<Long, String> segmentCyclicDependency(MetadataSegment segment, LinkedHashMap<Long, String> map) {
        LinkedHashMap<Long, String> segmentMap = (LinkedHashMap<Long, String>) map.clone();
        segmentMap.put(segment.getPid(), segment.getName());

        List<AttributeLookup> attributeLookups = findDependingAttributes(Collections.singletonList(segment));
        for (AttributeLookup attributeLookup : attributeLookups) {
            if (attributeLookup.getEntity() == BusinessEntity.Rating) {
                RatingEngine ratingEngine = ratingEngineService
                        .getRatingEngineById(RatingEngine.toEngineId(attributeLookup.getAttribute()), false);

                if (ratingEngine != null) {
                    MetadataSegment childSegment = ratingEngine.getSegment();
                    if (childSegment != null) {
                        if (segmentMap.containsKey(childSegment.getPid())) {
                            segmentMap.put(new Long(-1l), childSegment.getName());
                            return segmentMap;
                        } else {
                            return segmentCyclicDependency(childSegment, segmentMap);
                        }
                    }
                }
            }
        }

        return null;
    }

    @NoCustomerSpace
    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }
}
