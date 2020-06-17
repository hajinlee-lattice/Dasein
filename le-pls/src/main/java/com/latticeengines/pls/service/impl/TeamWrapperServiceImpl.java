package com.latticeengines.pls.service.impl;

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
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.service.TeamWrapperService;
import com.latticeengines.security.exposed.service.TeamService;

@Component("teamWrapperService")
public class TeamWrapperServiceImpl implements TeamWrapperService {

    @Inject
    private TeamService teamService;

    @Inject
    private BatonService batonService;

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

    @Override
    public List<GlobalTeam> getTeamsInContext(boolean withTeamMember, boolean appendDefaultGlobalTeam) {
        return teamService.getTeamsInContext(withTeamMember, appendDefaultGlobalTeam);
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
    public void fillTeamInfoForList(boolean inflateTeam, List<? extends HasTeamInfo> hasTeamInfos) {
        if (!inflateTeam) {
            return;
        }
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        if (teamFeatureEnabled) {
            Map<String, GlobalTeam> globalTeamMap = getTeamsInContext(false, true)
                    .stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
            Set<String> teamIds = getMyTeamIds();
            for (HasTeamInfo hasTeamInfo : hasTeamInfos) {
                TeamUtils.fillTeamId(hasTeamInfo);
                TeamUtils.fillTeamInfo(hasTeamInfo, globalTeamMap.get(hasTeamInfo.getTeamId()), teamIds);
            }
        }
    }
}
