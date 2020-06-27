package com.latticeengines.auth.exposed.util;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.HasTeamId;
import com.latticeengines.domain.exposed.auth.HasTeamInfo;
import com.latticeengines.domain.exposed.security.Session;

public final class TeamUtils {

    protected TeamUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String GLOBAL_TEAM = "Global Team";

    public static final String GLOBAL_TEAM_ID = "Global_Team";

    public static boolean isGlobalTeam(String teamId) {
        return StringUtils.isEmpty(teamId) || teamId.equals(GLOBAL_TEAM_ID);
    }

    private static final Logger log = LoggerFactory.getLogger(TeamUtils.class);

    public static boolean isMyTeam(String teamId) {
        if (!isGlobalTeam(teamId)) {
            Session session = MultiTenantContext.getSession();
            if (session != null) {
                List<String> teamIds = session.getTeamIds();
                log.info("Check team rights with teamId {} and teamIds in session context are {}.", teamId, teamIds);
                if (CollectionUtils.isEmpty(teamIds) || !teamIds.stream().collect(Collectors.toSet()).contains(teamId)) {
                    return false;
                } else {
                    return true;
                }
            } else {
                log.warn("Session doesn't exist in MultiTenantContext.");
                return true;
            }
        } else {
            log.info("Pass team rights check since teamId is empty.");
            return true;
        }
    }

    public static void fillTeamInfo(HasTeamInfo hasTeamInfo, GlobalTeam globalTeam, Set<String> teamIds) {
        if (globalTeam == null) {
            return;
        }
        hasTeamInfo.setTeam(globalTeam);
        String teamId = hasTeamInfo.getTeamId();
        if (!TeamUtils.isGlobalTeam(teamId) && !teamIds.contains(teamId)) {
            hasTeamInfo.setViewOnly(true);
        }
    }

    public static void fillTeamId(HasTeamId hasTeamId) {
        if (StringUtils.isEmpty(hasTeamId.getTeamId())) {
            hasTeamId.setTeamId(TeamUtils.GLOBAL_TEAM_ID);
        }
    }

    public static boolean shouldFailWhenAssignTeam(String createdBy, String currentUser, String orgTeamId, String teamId) {
        if (createdBy.equals(currentUser)) {
            return false;
        }
        if (StringUtils.isEmpty(orgTeamId)) {
            orgTeamId = GLOBAL_TEAM_ID;
        }
        if (orgTeamId.equals(teamId)) {
            return false;
        } else {
            return true;
        }
    }

}
