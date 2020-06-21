package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.HasTeamInfo;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.service.TeamWrapperService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.security.exposed.service.TeamService;

@Component("teamWrapperService")
public class TeamWrapperServiceImpl implements TeamWrapperService {

    @Inject
    private TeamService teamService;

    @Inject
    private BatonService batonService;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Override
    public Map<String, List<String>> getDependencies(String teamId) throws Exception {
        return teamService.getDependencies(teamId);
    }

    @Override
    public Boolean editTeam(String teamId, GlobalTeamData globalTeamData) {
        return teamService.editTeam(teamId, globalTeamData);
    }

    @Override
    public String createTeam(String createdByUser, GlobalTeamData globalTeamData) {
        return teamService.createTeam(createdByUser, globalTeamData);
    }

    private MetadataSegment simplifySegment(MetadataSegment metadataSegment) {
        MetadataSegment result = new MetadataSegment();
        result.setTeamId(metadataSegment.getTeamId());
        result.setName(metadataSegment.getName());
        result.setDisplayName(metadataSegment.getDisplayName());
        return result;
    }

    private Play simplifyPlay(Play play) {
        Play result = new Play();
        result.setName(play.getName());
        result.setDisplayName(play.getDisplayName());
        return result;
    }

    @Override
    public List<GlobalTeam> getTeams(boolean withTeamMember, boolean appendDefaultGlobalTeam) {
        String tenantId = MultiTenantContext.getTenant().getId();
        List<GlobalTeam> globalTeams = teamService.getTeamsInContext(withTeamMember, appendDefaultGlobalTeam);
        List<MetadataSegment> metadataSegments = segmentProxy.getMetadataSegments(tenantId).stream()
                .filter(s -> !Boolean.TRUE.equals(s.getMasterSegment())).map(metadataSegment -> simplifySegment(metadataSegment)).collect(Collectors.toList());
        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy.getAllRatingEngineSummaries(tenantId);
        List<Play> plays = playProxy.getPlays(tenantId);
        Map<String, List<MetadataSegment>> teamMapForSegment = metadataSegments.stream()
                .filter(metadataSegment -> !TeamUtils.isGlobalTeam(metadataSegment.getTeamId())).collect(Collectors.groupingBy(MetadataSegment::getTeamId));
        Map<String, List<RatingEngineSummary>> teamMapForRatingEngine = ratingEngineSummaries.stream()
                .filter(ratingEngineSummary -> !TeamUtils.isGlobalTeam(ratingEngineSummary.getTeamId())).collect(Collectors.groupingBy(RatingEngineSummary::getTeamId));
        Map<String, List<Play>> teamMapForPlay = plays.stream().filter(play -> !TeamUtils.isGlobalTeam(play.getTeamId()))
                .collect(Collectors.groupingBy(Play::getTeamId));
        teamMapForPlay = teamMapForPlay.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(),
                entry -> entry.getValue().stream().map(value -> simplifyPlay(value)).collect(Collectors.toList())));
        for (GlobalTeam globalTeam : globalTeams) {
            globalTeam.setMetadataSegments(teamMapForSegment.getOrDefault(globalTeam.getTeamId(), new ArrayList<>()));
            globalTeam.setRatingEngineSummaries(teamMapForRatingEngine.getOrDefault(globalTeam.getTeamId(), new ArrayList<>()));
            globalTeam.setPlays(teamMapForPlay.getOrDefault(globalTeam.getTeamId(), new ArrayList<>()));
        }
        return globalTeams;
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, User loginUser, boolean withTeamMember) {
        return teamService.getTeamsByUserName(username, loginUser, withTeamMember);
    }

    @Override
    public List<GlobalTeam> getMyTeams(boolean withTeamMember, boolean appendDefaultGlobalTeam) {
        return teamService.getMyTeams(withTeamMember, appendDefaultGlobalTeam);
    }

    @Override
    public Set<String> getMyTeamIds() {
        return teamService.getMyTeamIds();
    }

    @Override
    public GlobalTeam getTeamInContext(String teamId) {
        return teamService.getTeamInContext(teamId);
    }

    @Override
    public void fillTeamInfo(HasTeamInfo hasTeamInfo) {
        fillTeamInfo(hasTeamInfo, true);
    }

    @Override
    public void fillTeamInfo(HasTeamInfo hasTeamInfo, boolean setAllTeamFields) {
        if (hasTeamInfo == null) {
            return;
        }
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        if (teamFeatureEnabled) {
            TeamUtils.fillTeamId(hasTeamInfo);
            if (setAllTeamFields) {
                TeamUtils.fillTeamInfo(hasTeamInfo, getTeamInContext(hasTeamInfo.getTeamId()), getMyTeamIds());
            } else {
                hasTeamInfo.setViewOnly(!TeamUtils.isMyTeam(hasTeamInfo.getTeamId()));
            }
        }
    }

    @Override
    public void fillTeamInfoForList(List<? extends HasTeamInfo> hasTeamInfos) {
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        if (teamFeatureEnabled) {
            Map<String, GlobalTeam> globalTeamMap = teamService.getTeamsInContext(false, true)
                    .stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
            Set<String> teamIds = getMyTeamIds();
            for (HasTeamInfo hasTeamInfo : hasTeamInfos) {
                TeamUtils.fillTeamId(hasTeamInfo);
                TeamUtils.fillTeamInfo(hasTeamInfo, globalTeamMap.get(hasTeamInfo.getTeamId()), teamIds);
            }
        }
    }
}
