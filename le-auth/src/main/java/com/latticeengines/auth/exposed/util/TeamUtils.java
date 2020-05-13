package com.latticeengines.auth.exposed.util;

import org.apache.commons.lang3.StringUtils;

public final class TeamUtils {

    protected TeamUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String GLOBAL_TEAM = "Global Team";

    public static final String GLOBAL_TEAM_ID = "Global_Team";

    public static boolean isGlobalTeam(String teamId) {
        return StringUtils.isEmpty(teamId) || teamId.equals(GLOBAL_TEAM_ID);
    }

}
