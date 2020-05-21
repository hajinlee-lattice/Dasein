package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
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
import com.latticeengines.security.exposed.service.TeamService;

@Service("metadataSegmentService")
public class MetadataSegmentServiceImpl implements MetadataSegmentService {
    private static final String SEGMENT_IN_USE_TITLE = "Segment In Use";

    private static final String SEGMENT_DELETION_FAILED_GENERIC = "Segment deletion failed";

    private static final String SEGMENT_DELETE_FAILED_DEPENDENCY = "This segment is in use and cannot be deleted until the dependency has been removed.";

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentServiceImpl.class);

    private final SegmentProxy segmentProxy;

    private final ServingStoreCacheService servingStoreCacheService;

    private final GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    private final TeamService teamService;

    private final BatonService batonService;

    @Inject
    public MetadataSegmentServiceImpl(SegmentProxy segmentProxy, ServingStoreCacheService servingStoreCacheService,
                                      GraphDependencyToUIActionUtil graphDependencyToUIActionUtil,
                                      TeamService teamService, BatonService batonService) {
        this.segmentProxy = segmentProxy;
        this.servingStoreCacheService = servingStoreCacheService;
        this.graphDependencyToUIActionUtil = graphDependencyToUIActionUtil;
        this.teamService = teamService;
        this.batonService = batonService;
    }

    @Override
    public List<MetadataSegment> getSegments() {
        return getSegments(false);
    }

    private List<MetadataSegment> getSegments(boolean filter) {
        try (PerformanceTimer timer = new PerformanceTimer(String.format("Call getSegments with filter %s.", filter))) {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            List<MetadataSegment> backendSegments = segmentProxy.getMetadataSegments(customerSpace);
            if (CollectionUtils.isEmpty(backendSegments)) {
                return backendSegments;
            } else {
                Map<String, GlobalTeam> globalTeamMap;
                Set<String> teamIds = teamService.getTeamIdsInContext();
                boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
                if (teamFeatureEnabled) {
                    if (filter) {
                        globalTeamMap = teamService.getTeamsFromSession(false, true)
                                .stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
                        return backendSegments.stream().filter(segment -> StringUtils.isEmpty(segment.getTeamId()) || globalTeamMap.containsKey(segment.getTeamId())) //
                                .map(segment -> translateForFrontend(segment, globalTeamMap.get(segment.getTeamId()), teamIds))
                                .sorted((seg1, seg2) -> Boolean.compare( //
                                        Boolean.TRUE.equals(seg1.getMasterSegment()), //
                                        Boolean.TRUE.equals(seg2.getMasterSegment()) //
                                )).collect(Collectors.toList());
                    } else {
                        globalTeamMap = teamService.getTeamsInContext(true, true)
                                .stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
                    }
                } else {
                    globalTeamMap = new HashMap<>();
                }
                return backendSegments.stream() //
                        .map(segment -> translateForFrontend(segment, globalTeamMap.get(segment.getTeamId()), teamIds))
                        .sorted((seg1, seg2) -> Boolean.compare( //
                                Boolean.TRUE.equals(seg1.getMasterSegment()), //
                                Boolean.TRUE.equals(seg2.getMasterSegment()) //
                        )).collect(Collectors.toList());
            }
        }
    }

    @Override
    public List<MetadataSegment> getSegmentsInContext() {
        return getSegments(true);
    }

    @Override
    public MetadataSegment getSegmentByName(String name) {
        return getSegmentByName(name, true);
    }

    @Override
    public MetadataSegment getSegmentByName(String name, boolean shouldTranslateForFrontend) {
        try (PerformanceTimer timer = new PerformanceTimer(String.format("Get segment by name %s.", name))) {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace, name);
            if (shouldTranslateForFrontend && segment != null) {
                boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
                segment = translateForFrontend(segment, teamFeatureEnabled ?
                        teamService.getTeamInContext(segment.getTeamId()) : null, teamService.getTeamIdsInContext());
            }
            return segment;
        }
    }

    @Override
    public MetadataSegmentDTO getSegmentDTOByName(String name, boolean shouldTranslateForFrontend) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        MetadataSegmentDTO segmentDTO = segmentProxy.getMetadataSegmentWithPidByName(customerSpace, name);
        if (shouldTranslateForFrontend) {
            segmentDTO.setMetadataSegment(translateForFrontend(segmentDTO.getMetadataSegment(), null, null));
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
        Restriction accountRestriction = //
                RestrictionUtils.cleanupBucketsInRestriction(translatedSegment.getAccountRestriction());
        Restriction contactRestriction = //
                RestrictionUtils.cleanupBucketsInRestriction(translatedSegment.getContactRestriction());
        RestrictionUtils.validateCentralEntity(accountRestriction, BusinessEntity.Account);
        RestrictionUtils.validateCentralEntity(contactRestriction, BusinessEntity.Contact);
        translatedSegment.setAccountRestriction(accountRestriction);
        translatedSegment.setContactRestriction(contactRestriction);
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
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        MetadataSegment createdOrUpdatedSegment = translateForFrontend(metadataSegment, teamFeatureEnabled ?
                teamService.getTeamInContext(metadataSegment.getTeamId()) : null, teamService.getTeamIdsInContext());
        clearRatingCache();
        return createdOrUpdatedSegment;
    }

    @Override
    public Map<BusinessEntity, Long> updateSegmentCounts(String segmentName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Map<BusinessEntity, Long> counts = null;
        try {
            log.info("Updating entity counts for segment " + segmentName);
            counts = segmentProxy.updateSegmentCounts(customerSpace, segmentName);
        } catch (Exception e) {
            log.warn("Failed to update entity counts for segment " + segmentName);
        }
        return counts;
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

    private MetadataSegment translateForFrontend(MetadataSegment segment, GlobalTeam globalTeam, Set<String> teamIds) {
        if (segment == null) {
            return null;
        }
        try {
            segment.setTeam(globalTeam);
            String teamId = segment.getTeamId();
            if (StringUtils.isNotEmpty(teamId) && !teamIds.contains(teamId)) {
                segment.setViewOnly(true);
            }
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
