package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentAndActionDTO;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;

@Service("metadataSegmentService")
public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentServiceImpl.class);

    private final SegmentProxy segmentProxy;

    private final ActionService actionService;

    private final ServingStoreCacheService servingStoreCacheService;

    @Inject
    public MetadataSegmentServiceImpl(SegmentProxy segmentProxy, ActionService actionService,
            ServingStoreCacheService servingStoreCacheService) {
        this.segmentProxy = segmentProxy;
        this.actionService = actionService;
        this.servingStoreCacheService = servingStoreCacheService;
    }

    @Override
    public List<MetadataSegment> getSegments() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        List<MetadataSegment> backendSegments = segmentProxy.getMetadataSegments(customerSpace);
        if (backendSegments == null) {
            return null;
        } else {
            return backendSegments.stream() //
                    .map(this::translateForFrontend)
                    .sorted((seg1, seg2) -> Boolean.compare( //
                            Boolean.TRUE.equals(seg1.getMasterSegment()), //
                            Boolean.TRUE.equals(seg2.getMasterSegment()) //
                    )) //
                    .collect(Collectors.toList());
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
        segment = translateForBackend(segment);
        MetadataSegmentAndActionDTO metadataSegmentAndAction = segmentProxy
                .createOrUpdateSegmentAndActionDTO(customerSpace, segment);
        Action action = metadataSegmentAndAction.getAction();
        registerAction(action, MultiTenantContext.getTenant());
        MetadataSegment createdOrUpdatedSegment = translateForFrontend(metadataSegmentAndAction.getMetadataSegment());
        String segmentName = createdOrUpdatedSegment.getName();
        try {
            Thread.sleep(500);
            log.info("Updating entity counts for segment " + segmentName);
            Map<BusinessEntity, Long> counts = segmentProxy.updateSegmentCounts(customerSpace, segmentName);
            counts.forEach(createdOrUpdatedSegment::setEntityCount);
        } catch (Exception e) {
            log.warn("Failed to update entity counts for segment " + segmentName);
        }
        clearRatingCache();
        return createdOrUpdatedSegment;
    }

    @Override
    public void deleteSegmentByName(String name) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        segmentProxy.deleteSegmentByName(customerSpace, name);
    }

    private MetadataSegment translateForBackend(MetadataSegment segment) {
        try {
            FrontEndRestriction accountFrontEndRestriction = segment.getAccountFrontEndRestriction();
            if (accountFrontEndRestriction != null) {
                segment.setAccountRestriction(accountFrontEndRestriction.getRestriction());
                segment.setAccountFrontEndRestriction(null);
            }
            FrontEndRestriction contactFrontEndRestriction = segment.getContactFrontEndRestriction();
            if (contactFrontEndRestriction != null) {
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
            if (accountRestriction == null) {
                segment.setAccountFrontEndRestriction(emptyFrontEndRestriction());
            }
            Restriction contactRestriction = segment.getContactRestriction();
            if (contactRestriction == null) {
                segment.setContactFrontEndRestriction(emptyFrontEndRestriction());
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

    private void registerAction(Action action, Tenant tenant) {
        if (action != null) {
            action.setTenant(tenant);
            log.info(String.format("Registering action %s", action));
            ActionConfiguration actionConfig = action.getActionConfiguration();
            if (actionConfig != null) {
                action.setDescription(actionConfig.serialize());
            }
            actionService.create(action);
        }
    }

    private void clearRatingCache() {
        String tenantId = MultiTenantContext.getShortTenantId();
        servingStoreCacheService.clearCache(tenantId, BusinessEntity.Rating);
    }

    @Override
    public Map<CDLObjectTypes, List<String>> getDependencies(String segmentName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return segmentProxy.getDependencies(customerSpace, segmentName);
    }

}
