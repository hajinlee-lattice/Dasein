package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

@Aspect
public class PlsValidateTeamMemberRightsAspect {

    private static final Logger log = LoggerFactory.getLogger(PlsValidateTeamMemberRightsAspect.class);

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private PlayProxy playProxy;

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentServiceImpl.createOrUpdateSegment(..))")
    public void createOrUpdateSegment(JoinPoint joinPoint) {
        MetadataSegment segment = (MetadataSegment) joinPoint.getArgs()[0];
        checkTeamOnSegment(segment);
    }

    private void checkTeamOnSegment(MetadataSegment segment) {
        if (segment != null) {
            checkTeamInContext(segment.getTeamId());
        }
    }

    private void checkTeamInContext(String teamId) {
        if (StringUtils.isNotEmpty(teamId)) {
            Session session = MultiTenantContext.getSession();
            if (session != null) {
                List<String> teamIds = session.getTeamIds();
                log.info("Check team rights with teamId {} and teamIds in session context are {}.", teamId, teamIds);
                if (CollectionUtils.isEmpty(teamIds) || !teamIds.stream().collect(Collectors.toSet()).contains(teamId)) {
                    throw new AccessDeniedException("Access denied.");
                }
            } else {
                log.warn("Session doesn't exist in MultiTenantContext.");
            }
        } else {
            log.info("Pass team rights check since teamId is empty.");
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentServiceImpl.delete*(..))")
    public void deleteSegment(JoinPoint joinPoint) {
        String segmentName = (String) joinPoint.getArgs()[0];
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(MultiTenantContext.getTenant().getId(), segmentName);
        checkTeamOnSegment(segment);
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentExportServiceImpl.createSegmentExportJob(..))")
    public void createSegmentExportJob(JoinPoint joinPoint) {
        MetadataSegmentExport metadataSegmentExport = (MetadataSegmentExport) joinPoint.getArgs()[0];
        checkTeamInContext(metadataSegmentExport.getTeamId());
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.createOrUpdate(..))")
    public void createOrUpdatePlay(JoinPoint joinPoint) {
        Play play = (Play) joinPoint.getArgs()[0];
        if (play.getTargetSegment() == null) {
            checkTeamOnPlay(play.getName());
        } else {
            String segmentName = play.getTargetSegment().getName();
            MetadataSegment metadataSegment = segmentProxy.getMetadataSegmentByName(MultiTenantContext.getTenant().getId(), segmentName);
            checkTeamOnSegment(metadataSegment);
        }
    }

    private void checkTeamOnPlay(Play play) {
        checkTeamInContext(play.getTargetSegment().getTeamId());
    }

    private void checkTeamOnPlay(String playName) {
        Play play = playProxy.getPlay(MultiTenantContext.getTenant().getId(), playName);
        if (play != null) {
            checkTeamOnPlay(play);
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.delete(..))")
    public void deletePlay(JoinPoint joinPoint) {
        String playName = (String) joinPoint.getArgs()[0];
        checkTeamOnPlay(playName);
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.createPlayLaunchChannel(..))" +
            " ||execution(public * com.latticeengines.pls.service.impl.PlayServiceImpl.updatePlayLaunchChannel(..))")
    public void createOrUpdatePlayChannel(JoinPoint joinPoint) {
        String playName = (String) joinPoint.getArgs()[0];
        checkTeamOnPlay(playName);
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.TalkingPointServiceImpl.createOrUpdate(..))")
    public void createOrUpdateTalkingPoint(JoinPoint joinPoint) {
        List<TalkingPointDTO> talkingPoints = (List<TalkingPointDTO>) joinPoint.getArgs()[0];
        if (CollectionUtils.isNotEmpty(talkingPoints) && talkingPoints.size() > 0) {
            checkTeamOnPlay(talkingPoints.get(0).getPlayName());
        }
    }

    @Before("execution(public * com.latticeengines.pls.service.impl.TalkingPointServiceImpl.publish(..))" +
            " ||execution(public * com.latticeengines.pls.service.impl.TalkingPointServiceImpl.revert(..))")
    public void publishOrRevertTalkingPoint(JoinPoint joinPoint) {
        String playName = (String) joinPoint.getArgs()[0];
        checkTeamOnPlay(playName);
    }
}
