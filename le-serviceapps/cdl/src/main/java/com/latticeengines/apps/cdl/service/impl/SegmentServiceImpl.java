package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
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

    @Override
    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment segment) {
        if (segmentCyclicDependency(Collections.singletonList(segment))) {
            throw new LedpException(LedpCode.LEDP_40025);
        }
        MetadataSegment segment1 = segmentEntityMgr.createOrUpdateSegment(segment);
        evictRatingMetadataCache();
        return segment1;
    }

    @NoCustomerSpace
    private MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        if (segmentCyclicDependency(Collections.singletonList(segment))) {
            throw new LedpException(LedpCode.LEDP_40025);
        }
        MetadataSegment segment1 = segmentEntityMgr.createOrUpdateSegment(segment);
        evictRatingMetadataCache();
        return segment1;
    }

    @Override
    public Boolean deleteSegmentByName(String customerSpace, String segmentName) {
        MetadataSegment segment = segmentEntityMgr.findByName(segmentName);
        if (segment == null) {
            return false;
        }
        segmentEntityMgr.delete(segment);
        return true;
    }

    @Override
    public List<MetadataSegment> getSegments(String customerSpace) {
        String collectionName = dataCollectionEntityMgr.findOrCreateDefaultCollection().getName();
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
    public void deleteAllSegments(String customerSpace) {
        List<MetadataSegment> segments = getSegments(customerSpace);
        for (MetadataSegment segment : segments) {
            deleteSegmentByName(customerSpace, segment.getName());
        }
    }

    @Override
    @NoCustomerSpace
    public Map<BusinessEntity, Long> updateSegmentCounts(String segmentName) {
        Map<BusinessEntity, Long> map = new HashMap<>();
        MetadataSegment segment = findByName(segmentName);
        if (segment != null) {
            updateEntityCounts(segment);
            log.info("Updating counts for segment " + segmentName + " to "
                    + JsonUtils.serialize(segment.getEntityCounts()));
            segment = createOrUpdateSegment(segment);
            map = segment.getEntityCounts();
        }
        return map;
    }

    @NoCustomerSpace
    private void updateEntityCounts(MetadataSegment segment) {
        // use a deep copy to avoid changing restriction format to break UI
        MetadataSegment segmentCopy = JsonUtils.deserialize(JsonUtils.serialize(segment), MetadataSegment.class);
        Map<BusinessEntity, Long> counts = getEntityCounts(segmentCopy);
        counts.forEach(segment::setEntityCount);
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

    private void evictRatingMetadataCache() {
        String tenantId = MultiTenantContext.getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.DataCloudCMCache);
    }

    @Override
    @NoCustomerSpace
    public List<AttributeLookup> findDependingAttributes (List<MetadataSegment> metadataSegments) {
        Set<AttributeLookup> dependingAttributes = new HashSet<>();
        if (metadataSegments != null) {
            for (MetadataSegment metadataSegment : metadataSegments) {
                findSegmentDependingAttributes(metadataSegment);
                dependingAttributes.addAll(metadataSegment.getSegmentAttributes());
            }
        }

        return new ArrayList<>(dependingAttributes);
    }

    @Override
    public List<MetadataSegment> findDependingSegments (String customerSpace, List<String> attributes) {
        List<MetadataSegment> dependingMetadataSegments = new ArrayList<>();
        if (attributes != null) {
            List<MetadataSegment> metadataSegments = getSegments(customerSpace);
            if (metadataSegments != null) {
                for (MetadataSegment metadataSegment : metadataSegments) {
                    findSegmentDependingAttributes(metadataSegment);
                    Set<AttributeLookup> segmentAttributes = metadataSegment.getSegmentAttributes();
                    for (AttributeLookup attributeLookup : segmentAttributes) {
                        if (attributes.contains(sanitize(attributeLookup.toString()))) {
                            dependingMetadataSegments.add(metadataSegment);
                            break;
                        }
                    }
                }
            }
        }

        return dependingMetadataSegments;
    }

    @Override
    public boolean segmentCyclicDependency(List<MetadataSegment> metadataSegments) {
        boolean cyclicDependency = false;
        if (metadataSegments != null) {
            for (MetadataSegment metadataSegment : metadataSegments) {
                MetadataSegment existing = findByName(metadataSegment.getName());
                if (existing != null) {
                    cyclicDependency = segmentCyclicDependency(MultiTenantContext.getCustomerSpace().toString(),
                            metadataSegment, new ArrayList<>());
                    if (cyclicDependency) {
                        break;
                    }
                }
            }
        }

        return cyclicDependency;
    }

    private boolean segmentCyclicDependency(String customerSpace, MetadataSegment metadataSegment, List<Long> metadataSegmentList) {
        metadataSegmentList.add((metadataSegment.getPid()));
        List<AttributeLookup> attributeLookups = findDependingAttributes(Collections.singletonList(metadataSegment));
        if (attributeLookups != null) {
            for (AttributeLookup attributeLookup : attributeLookups) {
                List<MetadataSegment> childMetadataSegments = findDependingSegments(customerSpace,
                        Collections.singletonList(sanitize(attributeLookup.toString())));

                if (childMetadataSegments != null) {
                    for (MetadataSegment childMetadataSegment : childMetadataSegments) {
                        if (!metadataSegment.getPid().equals(childMetadataSegment.getPid())) {
                            return metadataSegmentList.contains(childMetadataSegment.getPid()) ||
                                    segmentCyclicDependency(customerSpace, childMetadataSegment, metadataSegmentList);
                        }
                    }
                }
            }
        }

        return false;
    }

    @NoCustomerSpace
    private void findSegmentDependingAttributes(MetadataSegment metadataSegment) {
        Set<AttributeLookup> segmentAttributes = new HashSet<>();
        Set<Restriction> restrictions = getSegmentRestrictions(metadataSegment);
        for (Restriction restriction : restrictions) {
            segmentAttributes.addAll(RestrictionUtils.getRestrictionDependingAttributes(restriction));
        }

        metadataSegment.setSegmentAttributes(segmentAttributes);
    }

    @NoCustomerSpace
    private Set<Restriction> getSegmentRestrictions (MetadataSegment metadataSegment) {
        Set<Restriction> restrictionSet = new HashSet<>();

        Restriction accountRestriction = metadataSegment.getAccountRestriction();
        if (accountRestriction != null) {
            restrictionSet.add(accountRestriction);
        }

        Restriction contactRestriction = metadataSegment.getContactRestriction();
        if (contactRestriction != null) {
            restrictionSet.add(contactRestriction);
        }

        FrontEndRestriction accountFrontEndRestriction = metadataSegment.getAccountFrontEndRestriction();
        if (accountFrontEndRestriction != null) {
            Restriction accountRestriction1 = accountFrontEndRestriction.getRestriction();
            if (accountRestriction1 != null) {
                restrictionSet.add(accountRestriction1);
            }
        }

        FrontEndRestriction contactFrontEndRestriction = metadataSegment.getContactFrontEndRestriction();
        if (contactFrontEndRestriction != null) {
            Restriction contactRestriction1 = contactFrontEndRestriction.getRestriction();
            if (contactRestriction1 != null) {
                restrictionSet.add(contactRestriction1);
            }
        }

        return restrictionSet;
    }

    @NoCustomerSpace
    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }
}
