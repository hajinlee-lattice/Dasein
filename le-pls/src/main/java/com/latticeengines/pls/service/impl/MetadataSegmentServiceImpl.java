package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Service("metadataSegmentService")
public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentServiceImpl.class);

    private final SegmentProxy segmentProxy;
    private final EntityProxy entityProxy;
    private final RatingEngineProxy ratingEngineProxy;

    @Inject
    public MetadataSegmentServiceImpl(SegmentProxy segmentProxy, EntityProxy entityProxy,
            RatingEngineProxy ratingEngineProxy) {
        this.segmentProxy = segmentProxy;
        this.entityProxy = entityProxy;
        this.ratingEngineProxy = ratingEngineProxy;
    }

    @Override
    public List<MetadataSegment> getSegments() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        List<MetadataSegment> backendSegments = segmentProxy.getMetadataSegments(customerSpace);
        if (backendSegments == null) {
            return null;
        } else {
            return segmentProxy.getMetadataSegments(customerSpace).stream() //
                    .map(this::translateForFrontend).collect(Collectors.toList());
        }
    }

    @Override
    public MetadataSegment getSegmentByName(String name) {
        return getSegmentByName(name, true);
    }

    @Override
    public MetadataSegment getSegmentByName(String name, boolean shouldTransateForFrontend) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace, name);
        if (shouldTransateForFrontend) {
            segment = translateForFrontend(segment);
        }
        return segment;
    }

    @Override
    public MetadataSegmentDTO getSegmentDTOByName(String name, boolean shouldTransateForFrontend) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        MetadataSegmentDTO segmentDTO = segmentProxy.getMetadataSegmentWithPidByName(customerSpace, name);
        if (shouldTransateForFrontend) {
            segmentDTO.setMetadataSegment(translateForFrontend(segmentDTO.getMetadataSegment()));
        }
        return segmentDTO;
    }

    @Override
    public MetadataSegment createOrUpdateSegment(MetadataSegment segment) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        if (Boolean.TRUE.equals(segment.getMasterSegment())) {
            throw new UnsupportedOperationException("Cannot change master segment.");
        }
        updateEntityCounts(segment);
        translateForBackend(segment);
        MetadataSegment updatedSegment = translateForFrontend(
                segmentProxy.createOrUpdateSegment(customerSpace, segment));
        updateRatingEngineCounts(customerSpace, updatedSegment.getName());
        return updatedSegment;
    }

    @Override
    public void deleteSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        segmentProxy.deleteSegmentByName(customerSpace, name);
    }

    private MetadataSegment translateForBackend(MetadataSegment segment) {
        try {
            FrontEndRestriction accountFrontEndRestriction = segment.getAccountFrontEndRestriction();
            if (segment.getAccountRestriction() == null) {
                segment.setAccountRestriction(accountFrontEndRestriction.getRestriction());
                segment.setAccountFrontEndRestriction(null);
            }
            FrontEndRestriction contactFrontEndRestriction = segment.getContactFrontEndRestriction();
            if (segment.getContactRestriction() == null) {
                segment.setContactRestriction(contactFrontEndRestriction.getRestriction());
                segment.setContactFrontEndRestriction(null);
            }
        } catch (Exception e) {
            log.error("Encountered error translating frontend restriction for segment with name " + segment.getName(),
                    e);
        }
        return segment;
    }

    private MetadataSegment translateForFrontend(MetadataSegment segment) {
        if (segment == null) {
            return null;
        }
        try {
            Restriction accountRestriction = segment.getAccountRestriction();
            if (accountRestriction != null) {
                FrontEndRestriction accountFrontEndRestriction = new FrontEndRestriction();
                accountFrontEndRestriction.setRestriction(accountRestriction);
                segment.setAccountFrontEndRestriction(accountFrontEndRestriction);
                segment.setAccountRestriction(null);
            } else {
                segment.setAccountFrontEndRestriction(emptyFrontEndRestriction());
            }
            Restriction contactRestriction = segment.getContactRestriction();
            if (contactRestriction != null) {
                FrontEndRestriction contactFrontEndRestriction = new FrontEndRestriction();
                contactFrontEndRestriction.setRestriction(contactRestriction);
                segment.setContactFrontEndRestriction(contactFrontEndRestriction);
                segment.setContactRestriction(null);
            }
            if (Boolean.FALSE.equals(segment.getMasterSegment())) {
                segment.setMasterSegment(null);
            }
        } catch (Exception e) {
            log.error("Encountered error translating backend restriction for segment with name  " + segment.getName(),
                    e);
        }

        return segment;
    }

    private FrontEndRestriction emptyFrontEndRestriction() {
        Restriction restriction = Restriction.builder().and(Collections.emptyList()).build();
        return new FrontEndRestriction(restriction);
    }

    private void updateEntityCounts(MetadataSegment segment) {
        // use a deep copy to avoid changing restriction format to break UI
        MetadataSegment segmentCopy = JsonUtils.deserialize(JsonUtils.serialize(segment), MetadataSegment.class);
        Map<BusinessEntity, Long> counts = getEntityCounts(segmentCopy);
        counts.forEach(segment::setEntityCount);
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

    private void updateRatingEngineCounts(String customerSpace, String segmentName) {
        List<String> engineIds = ratingEngineProxy.getRatingEngineIdsInSegment(customerSpace, segmentName);
        if (CollectionUtils.isNotEmpty(engineIds)) {
            log.info("There are " + engineIds.size() + " rating engines to update counts: "
                    + StringUtils.join(engineIds, ", "));
            engineIds.forEach(engineId -> {
                Map<String, Long> counts = ratingEngineProxy.updateRatingEngineCounts(customerSpace, engineId);
                log.info("Updated counts for rating engine " + engineId + " to " + JsonUtils.serialize(counts));
            });
        }

    }

}
