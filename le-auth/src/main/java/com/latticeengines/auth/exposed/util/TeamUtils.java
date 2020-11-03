package com.latticeengines.auth.exposed.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.HasTeamId;
import com.latticeengines.domain.exposed.auth.HasTeamInfo;
import com.latticeengines.domain.exposed.db.HasAuditUser;
import com.latticeengines.domain.exposed.security.Session;

public final class TeamUtils {

    protected TeamUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String GLOBAL_TEAM = "Global Team";

    public static final String GLOBAL_TEAM_ID = "Global_Team";

    private static final List<String> TEAM_REGARDLESS_ROLES_DCP = Arrays.asList( //
            "SUPER_ADMIN", "INTERNAL_ADMIN", "EXTERNAL_ADMIN");

    private static final Logger log = LoggerFactory.getLogger(TeamUtils.class);

    public static boolean isMyTeam(String teamId) {
        if (StringUtils.isEmpty(teamId)) {
            log.info("Team id is null, set it to global team id.");
            teamId = GLOBAL_TEAM_ID;
        }
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
            return false;
        }
    }

    public static void fillTeamInfo(HasTeamInfo hasTeamInfo, GlobalTeam globalTeam, Set<String> teamIds) {
        if (globalTeam == null) {
            return;
        }
        hasTeamInfo.setTeam(globalTeam);
        String teamId = hasTeamInfo.getTeamId();
        if (!teamIds.contains(teamId)) {
            hasTeamInfo.setViewOnly(true);
        }
    }

    public static void fillTeamId(HasTeamId hasTeamId) {
        if (hasTeamId != null && StringUtils.isEmpty(hasTeamId.getTeamId())) {
            hasTeamId.setTeamId(TeamUtils.GLOBAL_TEAM_ID);
        }
    }

    public static boolean shouldFailWhenAssignTeam(String id, Function<String, HasTeamInfo> queryFuc, HasTeamInfo current) {
        HasTeamInfo old = queryFuc.apply(id);
        if (old != null) {
            String loginUser = getLoginUser();
            HasAuditUser hasAuditUser = (HasAuditUser) old;
            String createdBy = hasAuditUser.getCreatedBy();
            if (createdBy.equals(loginUser)) {
                return false;
            }
            String oldTeamId = old.getTeamId();
            String teamId = current.getTeamId();
            if (StringUtils.isEmpty(oldTeamId)) {
                oldTeamId = GLOBAL_TEAM_ID;
            }
            if (oldTeamId.equals(teamId)) {
                return false;
            } else {
                log.info("Action that user {} want to assign team from teamId {} to teamId {} on entity {} failed.", loginUser, oldTeamId, teamId, id);
                return true;
            }
        } else {
            return false;
        }
    }

    public static String getLoginUser() {
        Session session = MultiTenantContext.getSession();
        if (session != null) {
            return session.getEmailAddress();
        } else {
            return null;
        }
    }

    public static  List<String> getTeamIds() {
        if(TEAM_REGARDLESS_ROLES_DCP.contains(MultiTenantContext.getSession().getAccessLevel())){
            return null;
        } else {
            Session session = MultiTenantContext.getSession();
            if (session != null) {
                return session.getTeamIds();
            } else {
                return Collections.emptyList();
            }
        }
    }
}
