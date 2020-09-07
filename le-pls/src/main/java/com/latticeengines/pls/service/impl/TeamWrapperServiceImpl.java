package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
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
import com.latticeengines.domain.exposed.auth.TeamEntityList;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.service.TeamWrapperService;
import com.latticeengines.proxy.exposed.cdl.TeamProxy;
import com.latticeengines.security.exposed.service.TeamService;

@Component("teamWrapperService")
public class TeamWrapperServiceImpl implements TeamWrapperService {

    @Inject
    private TeamService teamService;

    @Inject
    private BatonService batonService;

    @Inject
    private TeamProxy teamProxy;

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
        result.setUpdated(metadataSegment.getUpdated());
        return result;
    }

    private Play simplifyPlay(Play play) {
        Play result = new Play();
        result.setName(play.getName());
        result.setDisplayName(play.getDisplayName());
        result.setUpdated(play.getUpdated());
        return result;
    }

    private Map<String, TeamInfo> extractTeamMap(List<? extends HasTeamInfo> hasTeamInfoList) {
        Map<String, TeamInfo> teamMap = new HashMap<>();
        for (HasTeamInfo hasTeamInfo : hasTeamInfoList) {
            String teamId = hasTeamInfo.getTeamId();
            teamMap.putIfAbsent(teamId, new TeamInfo(Long.MIN_VALUE, new ArrayList<>()));
            teamMap.get(teamId).getHasTeamInfos().add(hasTeamInfo);
            long updated = getLastUsed(hasTeamInfo);
            if (teamMap.get(teamId).getLastUsed() < updated) {
                teamMap.get(teamId).setLastUsed(updated);
            }
        }
        return teamMap;
    }

    private long getLastUsed(HasTeamInfo hasTeamInfo) {
        if (hasTeamInfo instanceof MetadataSegment) {
            return ((MetadataSegment)hasTeamInfo).getUpdated().getTime();
        } else if (hasTeamInfo instanceof Play) {
            return ((Play)hasTeamInfo).getUpdated().getTime();
        } else if (hasTeamInfo instanceof RatingEngineSummary) {
            return ((RatingEngineSummary)hasTeamInfo).getUpdated().getTime();
        }
        return Long.MIN_VALUE;
    }

    private TeamInfo getTeamInfo(String teamId, Map<String, TeamInfo> teamInfoMap, List<TeamInfo> teamInfoList) {
        TeamInfo teamInfo = teamInfoMap.getOrDefault(teamId, new TeamInfo(Long.MIN_VALUE, new ArrayList<>()));
        teamInfoList.add(teamInfo);
        return teamInfo;
    }

    @Override
    public List<GlobalTeam> getTeams(boolean withTeamMember) {
        String tenantId = MultiTenantContext.getTenant().getId();
        List<GlobalTeam> globalTeams = teamService.getTeamsInContext(withTeamMember);
        TeamEntityList teamEntityList = teamProxy.getTeamEntities(tenantId);
        List<MetadataSegment> metadataSegments = teamEntityList.getMetadataSegments().stream().filter(s -> !Boolean.TRUE.equals(s.getMasterSegment())).collect(Collectors.toList());
        List<RatingEngineSummary> ratingEngineSummaries = teamEntityList.getRatingEngineSummaries();
        List<Play> plays = teamEntityList.getPlays();
        Map<String, TeamInfo> teamMapForSegment = extractTeamMap(metadataSegments);
        Map<String, TeamInfo> teamMapForRatingEngine = extractTeamMap(ratingEngineSummaries);
        Map<String, TeamInfo> teamMapForPlay = extractTeamMap(plays);
        for (GlobalTeam globalTeam : globalTeams) {
            String teamId = globalTeam.getTeamId();
            List<TeamInfo> teamInfoList = new ArrayList<>();
            TeamInfo segmentValue = getTeamInfo(teamId, teamMapForSegment, teamInfoList);
            globalTeam.setMetadataSegments(Arrays.asList(segmentValue.getHasTeamInfos().toArray(new MetadataSegment[0])).stream()
                    .map(metadataSegment -> simplifySegment(metadataSegment)).collect(Collectors.toList()));
            TeamInfo ratingEngineValue = getTeamInfo(teamId, teamMapForRatingEngine, teamInfoList);
            globalTeam.setRatingEngineSummaries(Arrays.asList(ratingEngineValue.getHasTeamInfos().toArray(new RatingEngineSummary[0])));
            TeamInfo playValue = getTeamInfo(teamId, teamMapForPlay, teamInfoList);
            globalTeam.setPlays(Arrays.asList(playValue.getHasTeamInfos().toArray(new Play[0])).stream().map(play -> simplifyPlay(play)).collect(Collectors.toList()));
            long lastUsed = teamInfoList.stream().max(Comparator.comparing(teamInfo -> teamInfo.getLastUsed())).get().getLastUsed();
            if (lastUsed > 0) {
                globalTeam.setLastUsed(new Date(lastUsed));
            }
        }
        return globalTeams;
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, User loginUser, boolean withTeamMember) {
        return teamService.getTeamsByUserName(username, loginUser, withTeamMember);
    }

    @Override
    public List<GlobalTeam> getMyTeams(boolean withTeamMember) {
        return teamService.getMyTeams(withTeamMember);
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
            Map<String, GlobalTeam> globalTeamMap = teamService.getTeamsInContext(false)
                    .stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
            Set<String> teamIds = getMyTeamIds();
            for (HasTeamInfo hasTeamInfo : hasTeamInfos) {
                TeamUtils.fillTeamInfo(hasTeamInfo, globalTeamMap.get(hasTeamInfo.getTeamId()), teamIds);
            }
        }
    }

    private class TeamInfo {

        private long lastUsed;

        private List<HasTeamInfo> hasTeamInfos;

        TeamInfo(long lastUsed, List<HasTeamInfo> hasTeamInfos) {
            this.lastUsed = lastUsed;
            this.hasTeamInfos = hasTeamInfos;
        }

        public long getLastUsed() {
            return lastUsed;
        }

        public void setLastUsed(long lastUsed) {
            this.lastUsed = lastUsed;
        }

        public List<HasTeamInfo> getHasTeamInfos() {
            return hasTeamInfos;
        }

        public void setHasTeamInfos(List<HasTeamInfo> hasTeamInfos) {
            this.hasTeamInfos = hasTeamInfos;
        }
    }
}
