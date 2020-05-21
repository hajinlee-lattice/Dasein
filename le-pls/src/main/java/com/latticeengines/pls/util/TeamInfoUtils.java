package com.latticeengines.pls.util;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.HasTeamInfo;
import com.latticeengines.security.exposed.service.TeamService;

public final class TeamInfoUtils {

    protected TeamInfoUtils() {
        throw new UnsupportedOperationException();
    }

    public static void fillTeams(HasTeamInfo hasTeamInfo, GlobalTeam globalTeam, Set<String> teamIds) {
        if (globalTeam == null) {
            return;
        }
        hasTeamInfo.setTeam(globalTeam);
        String teamId = hasTeamInfo.getTeamId();
        if (StringUtils.isNotEmpty(teamId) && !teamIds.contains(teamId)) {
            hasTeamInfo.setViewOnly(true);
        }
    }

    public static void fillTeamsForList(List<? extends HasTeamInfo> hasTeamInfos, BatonService batonService,
                                        TeamService teamService) {
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        if (teamFeatureEnabled) {
            Map<String, GlobalTeam> globalTeamMap = teamService.getTeamsInContext(false, true)
                    .stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
            Set<String> teamIds = teamService.getTeamIdsInContext();
            for (HasTeamInfo hasTeamInfo : hasTeamInfos) {
                fillTeams(hasTeamInfo, globalTeamMap.get(hasTeamInfo.getTeamId()), teamIds);
            }
        }
    }

    public static void fillTeams(HasTeamInfo hasTeamInfo, BatonService batonService, TeamService teamService) {
        boolean teamFeatureEnabled = batonService.isEnabled(MultiTenantContext.getCustomerSpace(), LatticeFeatureFlag.TEAM_FEATURE);
        if (teamFeatureEnabled) {
            fillTeams(hasTeamInfo, teamService.getTeamInContext(hasTeamInfo.getTeamId()), teamService.getTeamIdsInContext());
        }
    }
}
