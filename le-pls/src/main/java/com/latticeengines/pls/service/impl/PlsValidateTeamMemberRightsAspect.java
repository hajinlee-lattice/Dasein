package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

@Aspect
public class PlsValidateTeamMemberRightsAspect {

    private static final Logger log = LoggerFactory.getLogger(PlsValidateTeamMemberRightsAspect.class);

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private BatonService batonService;

    private static final String updateRatingEngineMethod = "createOrUpdateRatingEngine";

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentServiceImpl.createOrUpdateSegment(..))")
    public void createOrUpdateSegment(JoinPoint joinPoint) {
        MetadataSegment segment = (MetadataSegment) joinPoint.getArgs()[0];
        checkTeamWithSegment(segment);
    }

    private void checkTeamWithSegment(MetadataSegment segment) {
        if (teamFeatureEnabled() && segment != null) {
            if (StringUtils.isNotEmpty(segment.getName()) && segment.getTeamId() != null) {
                String loginUser = getLoginUser();
                MetadataSegment orgSegment = segmentProxy.getMetadataSegmentByName(MultiTenantContext.getTenant().getId(), segment.getName());
                if (orgSegment != null && TeamUtils.shouldFailWhenAssignTeam(orgSegment.getCreatedBy(), loginUser, orgSegment.getTeamId(), segment.getTeamId())) {
                    throw new AccessDeniedException("Access denied.");
                }
            }
            checkTeamInContext(segment.getTeamId());
        }
    }

    private String getLoginUser() {
        Session session = MultiTenantContext.getSession();
        if (session != null) {
            return session.getEmailAddress();
        } else {
            return null;
        }
    }

    private void checkTeamWithSegmentName(String segmentName) {
        if (teamFeatureEnabled()) {
            MetadataSegment metadataSegment = segmentProxy.getMetadataSegmentByName(MultiTenantContext.getTenant().getId(), segmentName);
            if (metadataSegment != null) {
                checkTeamInContext(metadataSegment.getTeamId());
            }
        }
    }

    private void checkTeamInContext(String teamId) {
        if (!TeamUtils.isMyTeam(teamId)) {
            throw new AccessDeniedException("Access denied.");
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentServiceImpl.delete*(..))")
    public void deleteSegment(JoinPoint joinPoint) {
        String segmentName = (String) joinPoint.getArgs()[0];
        checkTeamWithSegmentName(segmentName);
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentExportServiceImpl.createSegmentExportJob(..))")
    public void createSegmentExportJob(JoinPoint joinPoint) {
        MetadataSegmentExport metadataSegmentExport = (MetadataSegmentExport) joinPoint.getArgs()[0];
        if (teamFeatureEnabled()) {
            checkTeamInContext(metadataSegmentExport.getTeamId());
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.createOrUpdate(..))")
    public void createOrUpdatePlay(JoinPoint joinPoint) {
        Play play = (Play) joinPoint.getArgs()[0];
        if (play.getTargetSegment() == null) {
            checkTeamWithPlayName(play.getName());
        } else {
            String segmentName = play.getTargetSegment().getName();
            checkTeamWithSegmentName(segmentName);
        }
    }

    private void checkTeamWithPlayName(String playName) {
        if (teamFeatureEnabled()) {
            Play play = playProxy.getPlay(MultiTenantContext.getTenant().getId(), playName);
            if (play != null) {
                checkTeamInContext(play.getTeamId());
            }
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.delete(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.TalkingPointServiceImpl.revert(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.TalkingPointServiceImpl.publish(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.createPlayLaunchChannel(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.updatePlayLaunchChannel(..))")
    public void checkWithPlayName(JoinPoint joinPoint) {
        String playName = (String) joinPoint.getArgs()[0];
        checkTeamWithPlayName(playName);
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.TalkingPointServiceImpl.createOrUpdate(..))")
    public void createOrUpdateTalkingPoint(JoinPoint joinPoint) {
        List<TalkingPointDTO> talkingPoints = (List<TalkingPointDTO>) joinPoint.getArgs()[0];
        if (CollectionUtils.isNotEmpty(talkingPoints) && talkingPoints.size() > 0) {
            checkTeamWithPlayName(talkingPoints.get(0).getPlayName());
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.RatingEngineServiceImpl.create*(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.RatingEngineServiceImpl.update*(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.RatingEngineServiceImpl.delete*(..))" +
            " || execution(public * com.latticeengines.pls.service.impl.RatingEngineServiceImpl.setScoringIteration(..))")
    public void crudForRatingEngine(JoinPoint joinPoint) {
        if (joinPoint.getArgs()[0] instanceof RatingEngine) {
            String methodName = joinPoint.getSignature().getName();
            RatingEngine ratingEngine = (RatingEngine) joinPoint.getArgs()[0];
            if (updateRatingEngineMethod.equals(methodName) && ratingEngine.getId() != null && ratingEngine.getTeamId() != null) {
                String loginUser = getLoginUser();
                RatingEngine oldRatingEngine = ratingEngineProxy.getRatingEngine(MultiTenantContext.getShortTenantId(), ratingEngine.getId());
                if (oldRatingEngine != null && TeamUtils.shouldFailWhenAssignTeam(oldRatingEngine.getCreatedBy(),
                        loginUser, oldRatingEngine.getTeamId(), ratingEngine.getTeamId())) {
                    throw new AccessDeniedException("Access denied.");
                }
            }
            if (StringUtils.isNotEmpty(ratingEngine.getTeamId())) {
                checkTeamWithRatingEngine(ratingEngine);
            } else if (StringUtils.isNotEmpty(ratingEngine.getId())) {
                checkTeamWithRatingEngineId(ratingEngine.getId());
            }
        } else if (joinPoint.getArgs()[0] instanceof String) {
            checkTeamWithRatingEngineId((String) joinPoint.getArgs()[0]);
        }
    }

    private void checkTeamWithRatingEngine(RatingEngine ratingEngine) {
        if (teamFeatureEnabled()) {
            checkTeamInContext(ratingEngine.getTeamId());
        }
    }

    private void checkTeamWithRatingEngineId(String ratingEngineId) {
        if (teamFeatureEnabled()) {
            RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(MultiTenantContext.getTenant().getId(), ratingEngineId);
            if (ratingEngine != null) {
                checkTeamInContext(ratingEngine.getTeamId());
            }
        }
    }

    private boolean teamFeatureEnabled() {
        return batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
    }
}
