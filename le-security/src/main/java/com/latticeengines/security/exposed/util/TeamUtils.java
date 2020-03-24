package com.latticeengines.security.exposed.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.service.TeamService;

public final class TeamUtils {

    protected TeamUtils() {
        throw new UnsupportedOperationException();
    }

    public static Map<String, GlobalTeam> getGlobalTeamMap(TeamService teamService, User loginUser) {
        List<GlobalTeam> globalTeams = teamService.getTeams(loginUser);
        Map<String, GlobalTeam> globalTeamMap = new HashMap<>();
        if (CollectionUtils.isEmpty(globalTeams)) {
            return globalTeamMap;
        } else {
            return globalTeams.stream().collect(Collectors.toMap(GlobalTeam::getTeamId, GlobalTeam -> GlobalTeam));
        }
    }

    public static GlobalTeam getGlobalTeam(TeamService teamService, String teamId) {
        if (StringUtils.isNotEmpty(teamId)) {
            return teamService.getTeamByTeamId(teamId, MultiTenantContext.getUser());
        } else {
            return null;
        }
    }
}
