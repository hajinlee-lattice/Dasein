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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;

@Service("metadataSegmentService")
public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final String SEGMENT_IN_USE_TITLE = "Segment In Use";

    private static final String SEGMENT_DELETION_FAILED_GENERIC = "Segment deletion failed";

    private static final String SEGMENT_DELETE_FAILED_DEPENDENCY = "This segment is in use and cannot be deleted until the dependency has been removed.";

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentServiceImpl.class);

    private final SegmentProxy segmentProxy;

    private final ServingStoreCacheService servingStoreCacheService;

    private final GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @Inject
    public MetadataSegmentServiceImpl(SegmentProxy segmentProxy, ServingStoreCacheService servingStoreCacheService,
            GraphDependencyToUIActionUtil graphDependencyToUIActionUtil) {
        this.segmentProxy = segmentProxy;
        this.servingStoreCacheService = servingStoreCacheService;
        this.graphDependencyToUIActionUtil = graphDependencyToUIActionUtil;
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
        MetadataSegment translatedSegment = translateForBackend(segment);
        /* temporarily revert this change due to PLS-13661
        translatedSegment.setAccountRestriction( //
                RestrictionUtils.cleanupBucketsInRestriction(translatedSegment.getAccountRestriction()));
        translatedSegment.setContactRestriction( //
                RestrictionUtils.cleanupBucketsInRestriction(translatedSegment.getContactRestriction()));
        */
        MetadataSegment metadataSegment;
        try {
            metadataSegment = segmentProxy.createOrUpdateSegment(customerSpace, translatedSegment,
                    MultiTenantContext.getEmailAddress());
        } catch (Exception ex) {
            if (ex instanceof LedpException && LedpCode.LEDP_40057.equals(((LedpException) ex).getCode())) {
                throw graphDependencyToUIActionUtil.handleInvalidBucketsError((LedpException) ex, //
                        "Failed to save or update segment");
            } else {
                throw graphDependencyToUIActionUtil.handleExceptionForCreateOrUpdate(ex, LedpCode.LEDP_40041);
            }
        }

        MetadataSegment createdOrUpdatedSegment = translateForFrontend(metadataSegment);
        String segmentName = createdOrUpdatedSegment.getName();
        try {
            Thread.sleep(100);
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
    public void deleteSegmentByName(String name, boolean hardDelete) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        segmentProxy.deleteSegmentByName(customerSpace, name, hardDelete);
    }

    @Override
    public void revertDeleteSegment(String segmentName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        segmentProxy.revertDeleteSegmentByName(customerSpace, segmentName);
    }

    @Override
    public List<String> getAllDeletedSegments() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return segmentProxy.getAllDeletedSegments(customerSpace);
    }

    @Override
    public Map<String, List<String>> getDependencies(String segmentName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return segmentProxy.getDependencies(customerSpace, segmentName);
    }

    @Override
    public UIAction getDependenciesModelAndView(String segmentName) {
        MetadataSegment segment = getSegmentByName(segmentName);
        if (segment == null) {
            log.warn(String.format("Cannot find segment with name %s", segmentName));
            return null;
        }
        Map<String, List<String>> dependencies = getDependencies(segmentName);
        return graphDependencyToUIActionUtil.processUpdateSegmentResponse(segment, dependencies);
    }

    @Override
    public UIAction deleteSegmentByNameModelAndView(String segmentName, boolean hardDelete) {
        UIAction uiAction = null;
        try {
            deleteSegmentByName(segmentName, hardDelete);
            uiAction = graphDependencyToUIActionUtil.generateUIAction("Segment is deleted successfully", View.Notice,
                    Status.Success, null);
        } catch (LedpException ex) {
            uiAction = graphDependencyToUIActionUtil.handleDeleteFailedDueToDependency(ex, LedpCode.LEDP_40042,
                    SEGMENT_IN_USE_TITLE, SEGMENT_DELETE_FAILED_DEPENDENCY, View.Modal, SEGMENT_DELETION_FAILED_GENERIC,
                    View.Banner);
        }
        return uiAction;
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

    private void clearRatingCache() {
        String tenantId = MultiTenantContext.getShortTenantId();
        servingStoreCacheService.clearCache(tenantId, BusinessEntity.Rating);
    }
}
