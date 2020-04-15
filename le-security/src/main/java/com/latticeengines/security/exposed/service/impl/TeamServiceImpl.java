package com.latticeengines.security.exposed.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.auth.exposed.service.impl.GlobalAuthDependencyChecker;
import com.latticeengines.auth.exposed.util.TeamUtils;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TeamService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;

@Component("teamService")
public class TeamServiceImpl implements TeamService {

    private static final Logger log = LoggerFactory.getLogger(TeamServiceImpl.class);

    @Inject
    private GlobalTeamManagementService globalTeamManagementService;

    @Inject
    private UserService userService;

    @Inject
    private GlobalAuthDependencyChecker dependencyChecker;

    @Override
    public List<GlobalTeam> getTeams(User loginUser) {
        return getTeams(loginUser, true);
    }

    @Override
    public List<GlobalTeam> getTeams(User loginUser, boolean withTeamMember) {
        try (PerformanceTimer timer = new PerformanceTimer(String.format("User %s requests to query teams.", loginUser.getEmail()))) {
            List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeams(withTeamMember);
            return getGlobalTeams(globalAuthTeams, withTeamMember, loginUser);
        }
    }

    private UserFilter getFilter(User loginUser) {
        if (isExternalUser(loginUser)) {
            return UserFilter.EXTERNAL_FILTER;
        } else {
            return UserFilter.TRIVIAL_FILTER;
        }
    }

    private boolean isExternalUser(User loginUser) {
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        if (loginLevel.equals(AccessLevel.EXTERNAL_USER) || loginLevel.equals(AccessLevel.EXTERNAL_ADMIN)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, User loginUser, boolean withTeamMember) {
        try (PerformanceTimer timer = new PerformanceTimer(String.format("Get teams by username %s.", username))) {
            List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeamsByUserName(username, withTeamMember);
            return getGlobalTeams(globalAuthTeams, withTeamMember, loginUser);
        }
    }

    @Override
    public List<GlobalTeam> getTeamsFromSession(boolean withTeamMember, boolean appendDefaultGlobalTeam) {
        try (PerformanceTimer timer = new PerformanceTimer("Get teams in session context.")) {
            List<GlobalTeam> globalTeams = new ArrayList<>();
            Session session = MultiTenantContext.getSession();
            if (session != null && CollectionUtils.isNotEmpty(session.getTeamIds())) {
                List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeamsByTeamIds(session.getTeamIds(), withTeamMember);
                globalTeams = getGlobalTeams(globalAuthTeams, withTeamMember, MultiTenantContext.getUser());
            }
            return appendDefaultGlobalTeam(appendDefaultGlobalTeam, globalTeams);
        }
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, User loginUser) {
        return getTeamsByUserName(username, loginUser, true);
    }

    @Override
    public GlobalTeam getTeamByTeamId(String teamId, User loginUser, boolean withTeamMember) {
        try (PerformanceTimer timer = new PerformanceTimer(String.format("Get team by teamId %s.", teamId))) {
            GlobalAuthTeam globalAuthTeam = globalTeamManagementService.getTeamById(teamId, withTeamMember);
            GlobalTeam globalTeam = null;
            if (globalAuthTeam != null) {
                List<GlobalAuthUserTenantRight> globalAuthUserTenantRights = globalAuthTeam.getUserTenantRights();
                Map<String, User> userMap;
                Set<String> emails = Sets.newHashSet(globalAuthTeam.getCreatedByUser());
                if (withTeamMember && CollectionUtils.isNotEmpty(globalAuthUserTenantRights)) {
                    for (GlobalAuthUserTenantRight globalAuthUserTenantRight : globalAuthUserTenantRights) {
                        emails.add(globalAuthUserTenantRight.getGlobalAuthUser().getEmail());
                    }
                }
                List<User> users = userService.getUsers(MultiTenantContext.getTenant().getId(), UserFilter.TRIVIAL_FILTER,
                        emails, false);
                userMap = users.stream().collect(Collectors.toMap(User::getEmail, User -> User));
                globalTeam = getGlobalTeam(globalAuthTeam, withTeamMember, userMap, loginUser);
            }
            return globalTeam;
        }
    }

    @Override
    public GlobalTeam getTeamByTeamId(String teamId, User loginUser) {
        return getTeamByTeamId(teamId, loginUser, true);
    }


    // generate a Global Team with teamId null, display for UI
    @Override
    public GlobalTeam getDefaultGlobalTeam() {
        GlobalTeam globalTeam = new GlobalTeam();
        globalTeam.setTeamName(TeamUtils.GLOBAL_TEAM);
        globalTeam.setTeamMembers(new ArrayList<>());
        return globalTeam;
    }

    private List<GlobalTeam> getGlobalTeams(List<GlobalAuthTeam> globalAuthTeams, boolean withTeamMember, User loginUser) {
        List<User> users = userService.getUsers(MultiTenantContext.getTenant().getId(), UserFilter.TRIVIAL_FILTER, false);
        Map<String, User> userMap = users.stream().collect(Collectors.toMap(User::getEmail, User -> User));
        List<GlobalTeam> globalTeams = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(globalAuthTeams)) {
            for (GlobalAuthTeam globalAuthTeam : globalAuthTeams) {
                globalTeams.add(getGlobalTeam(globalAuthTeam, withTeamMember, userMap, loginUser));
            }
        }
        return globalTeams;
    }

    private GlobalTeam getGlobalTeam(GlobalAuthTeam globalAuthTeam, boolean withTeamMember, Map<String, User> userMap, User loginUser) {
        GlobalTeam globalTeam = new GlobalTeam();
        globalTeam.setTeamName(globalAuthTeam.getName());
        globalTeam.setTeamId(globalAuthTeam.getTeamId());
        globalTeam.setCreated(globalAuthTeam.getCreationDate());
        globalTeam.setCreatedByUser(userMap.get(globalAuthTeam.getCreatedByUser()));
        if (withTeamMember) {
            UserFilter userFilter = getFilter(loginUser);
            List<User> teamMembers = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(globalAuthTeam.getUserTenantRights())) {
                for (GlobalAuthUserTenantRight globalAuthUserTenantRight : globalAuthTeam.getUserTenantRights()) {
                    User user = userMap.get(globalAuthUserTenantRight.getGlobalAuthUser().getEmail());
                    if (user != null && (userFilter == UserFilter.TRIVIAL_FILTER || userFilter.visible(user))) {
                        teamMembers.add(user);
                    }
                }
            }
            globalTeam.setTeamMembers(teamMembers);
        }
        return globalTeam;
    }

    @Override
    public String createTeam(String createdByUser, GlobalTeamData globalTeamData) {
        GlobalAuthTeam globalAuthTeam = globalTeamManagementService.createTeam(createdByUser, globalTeamData);
        userService.clearSession(false, MultiTenantContext.getTenant().getId(), getUserIds(globalAuthTeam));
        return globalAuthTeam.getTeamId();
    }

    @Override
    public Boolean editTeam(User loginUser, String teamId, GlobalTeamData globalTeamData) {
        GlobalAuthTeam globalAuthTeam = globalTeamManagementService.getTeamById(teamId, true);
        GlobalAuthTeam globalAuthTeamUpdated = globalTeamManagementService.updateTeam(teamId, globalTeamData);
        List<Long> userIds = getChangedUserNames(getUserIds(globalAuthTeam), getUserIds(globalAuthTeamUpdated));
        userService.clearSession(false, MultiTenantContext.getTenant().getId(), userIds);
        return true;
    }

    private List<Long> getUserIds(GlobalAuthTeam globalAuthTeam) {
        return globalAuthTeam.getUserTenantRights().stream()
                .map(globalAuthUserTenantRight -> globalAuthUserTenantRight.getGlobalAuthUser().getPid()).collect(Collectors.toList());
    }

    private List<Long> getChangedUserNames(List<Long> orgUserIds, List<Long> newUserIds) {
        List<Long> result = new ArrayList<>();
        Set<Long> userIds1 = new HashSet<>(orgUserIds);
        Set<Long> userIds2 = new HashSet<>(newUserIds);
        Set<Long> diffIds1 = userIds1.stream().filter(pid -> !userIds2.contains(pid)).collect(Collectors.toSet());
        Set<Long> diffIds2 = userIds2.stream().filter(pid -> !userIds1.contains(pid)).collect(Collectors.toSet());
        result.addAll(diffIds1);
        result.addAll(diffIds2);
        return result;
    }

    @Override
    public Boolean editTeam(String teamId, GlobalTeamData globalTeamData) {
        return editTeam(MultiTenantContext.getUser(), teamId, globalTeamData);
    }

    @Override
    public Boolean deleteTeam(String teamId) {
        globalTeamManagementService.deleteTeamByTeamId(teamId);
        return true;
    }

    @Override
    public Map<String, List<String>> getDependencies(String teamId) throws Exception {
        return dependencyChecker.getDependencies(teamId, CDLObjectTypes.Team.name());
    }

    @Override
    public boolean userBelongsToTeam(String username, String teamId) {
        try (PerformanceTimer timer = new PerformanceTimer(String.format("Call userBelongsToTeam with username %s and" +
                " teamId %s.", username, teamId))) {
            return globalTeamManagementService.userBelongsToTeam(username, teamId);
        }
    }

    private List<GlobalTeam> appendDefaultGlobalTeam(boolean appendDefaultGlobalTeam, List<GlobalTeam> globalTeams) {
        if (appendDefaultGlobalTeam) {
            List<GlobalTeam> globalTeams2 = new ArrayList<>();
            globalTeams2.add(getDefaultGlobalTeam());
            globalTeams2.addAll(globalTeams);
            return globalTeams2;
        } else {
            return globalTeams;
        }
    }

    @Override
    public List<GlobalTeam> getTeamsInContext(boolean withTeamMember, boolean appendDefaultGlobalTeam) {
        User loginUser = MultiTenantContext.getUser();
        List<GlobalTeam> globalTeams = getTeams(loginUser, withTeamMember);
        return appendDefaultGlobalTeam(appendDefaultGlobalTeam, globalTeams);
    }

    @Override
    public GlobalTeam getTeamInContext(String teamId) {
        if (StringUtils.isNotEmpty(teamId)) {
            return getTeamByTeamId(teamId, MultiTenantContext.getUser());
        } else {
            return getDefaultGlobalTeam();
        }
    }

    @Override
    public Set<String> getTeamIdsInContext() {
        Session session = MultiTenantContext.getSession();
        if (session != null) {
            return session.getTeamIds().stream().collect(Collectors.toSet());
        } else {
            return new HashSet<>();
        }
    }
}
