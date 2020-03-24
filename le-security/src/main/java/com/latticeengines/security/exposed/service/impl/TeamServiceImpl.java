package com.latticeengines.security.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.auth.exposed.service.impl.GlobalAuthDependencyChecker;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
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
        List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeams(true);
        return getGlobalTeams(globalAuthTeams, true, loginUser);
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

    private boolean isNonAdminUser(User loginUser) {
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        if (loginLevel.equals(AccessLevel.EXTERNAL_USER) || loginLevel.equals(AccessLevel.INTERNAL_USER)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isInternalUser(GlobalAuthUserTenantRight globalAuthUserTenantRight) {
        if (AccessLevel.INTERNAL_ADMIN.name().equals(globalAuthUserTenantRight.getOperationName())
                || AccessLevel.INTERNAL_USER.name().equals(globalAuthUserTenantRight.getOperationName())
                || AccessLevel.SUPER_ADMIN.name().equals(globalAuthUserTenantRight.getOperationName())) {
            return globalAuthUserTenantRight.getExpirationDate() == null || globalAuthUserTenantRight.getExpirationDate() > System.currentTimeMillis();
        } else {
            return false;
        }
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, User loginUser) {
        List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeamsByUserName(username, true);
        return getGlobalTeams(globalAuthTeams, true, loginUser);
    }

    @Override
    public GlobalTeam getTeamByTeamId(String teamId, User loginUser) {
        GlobalAuthTeam globalAuthTeam = globalTeamManagementService.getTeamById(teamId, true);
        GlobalTeam globalTeam = null;
        List<GlobalAuthUserTenantRight> globalAuthUserTenantRights = globalAuthTeam.getUserTenantRights();
        if (CollectionUtils.isNotEmpty(globalAuthUserTenantRights)) {
            List<User> users = userService.getUsers(MultiTenantContext.getTenant().getId(), getFilter(loginUser),
                    globalAuthUserTenantRights, false);
            Map<String, User> userMap = users.stream().collect(Collectors.toMap(User::getEmail, User -> User));
            globalTeam = getGlobalTeam(globalAuthTeam, true, userMap);
        }
        if (globalAuthTeam != null) {
            log.warn("There is no team with id {}", teamId);
        }
        return globalTeam;
    }

    private List<GlobalTeam> getGlobalTeams(List<GlobalAuthTeam> globalAuthTeams, boolean withTeamMember, User loginUser) {
        List<User> users = userService.getUsers(MultiTenantContext.getTenant().getId(), getFilter(loginUser), false);
        Map<String, User> userMap = users.stream().collect(Collectors.toMap(User::getEmail, User -> User));
        List<GlobalTeam> globalTeams = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(globalAuthTeams)) {
            for (GlobalAuthTeam globalAuthTeam : globalAuthTeams) {
                globalTeams.add(getGlobalTeam(globalAuthTeam, withTeamMember, userMap));
            }
        }
        return globalTeams;
    }

    private GlobalTeam getGlobalTeam(GlobalAuthTeam globalAuthTeam, boolean withTeamMember, Map<String, User> userMap) {
        GlobalTeam globalTeam = new GlobalTeam();
        globalTeam.setTeamName(globalAuthTeam.getName());
        globalTeam.setTeamId(globalAuthTeam.getTeamId());
        globalTeam.setCreated(globalAuthTeam.getCreationDate());
        globalTeam.setCreatedByUser(userMap.get(globalAuthTeam.getCreatedByUser()));
        if (withTeamMember) {
            List<User> teamMembers = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(globalAuthTeam.getUserTenantRights())) {
                for (GlobalAuthUserTenantRight globalAuthUserTenantRight : globalAuthTeam.getUserTenantRights()) {
                    User user = userMap.get(globalAuthUserTenantRight.getGlobalAuthUser().getEmail());
                    if (user != null) {
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
        return globalTeamManagementService.createTeam(createdByUser, globalTeamData);
    }

    @Override
    public Boolean editTeam(User loginUser, String teamId, GlobalTeamData globalTeamData) {
        if (isNonAdminUser(loginUser) && !globalTeamManagementService.userBelongsToTeam(loginUser.getEmail(), teamId)) {
            throw new AccessDeniedException("Access denied.");
        }
        if (CollectionUtils.isNotEmpty(globalTeamData.getTeamMembers()) && isExternalUser(loginUser)) {
            // add the internal users into team member list if internal user exists in the edit team
            GlobalAuthTeam globalAuthTeam = globalTeamManagementService.getTeamById(teamId, true);
            if (globalAuthTeam != null && CollectionUtils.isNotEmpty(globalAuthTeam.getUserTenantRights())) {
                List<GlobalAuthUserTenantRight> globalAuthUserTenantRights = globalAuthTeam.getUserTenantRights();
                Set<String> teamMembers = globalTeamData.getTeamMembers();
                for (GlobalAuthUserTenantRight globalAuthUserTenantRight : globalAuthUserTenantRights) {
                    String username = globalAuthUserTenantRight.getGlobalAuthUser().getEmail();
                    if (!teamMembers.contains(username) && isInternalUser(globalAuthUserTenantRight)) {
                        teamMembers.add(username);
                    }
                }
            }
        }
        globalTeamManagementService.updateTeam(teamId, globalTeamData);
        return true;
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
        return globalTeamManagementService.userBelongsToTeam(username, teamId);
    }

}
