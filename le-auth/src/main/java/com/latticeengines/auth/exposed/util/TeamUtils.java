package com.latticeengines.auth.exposed.util;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
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

}
