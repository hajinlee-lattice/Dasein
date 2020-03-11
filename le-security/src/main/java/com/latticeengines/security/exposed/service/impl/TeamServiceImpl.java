package com.latticeengines.security.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.UpdateTeamUsersRequest;
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

    @Override
    public List<GlobalTeam> getTeams(User loginUser) {
        List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeams(true);
        return getGlobalTeams(globalAuthTeams, true, loginUser);
    }

    private UserFilter getFilter(User loginUser) {
        AccessLevel loginLevel = AccessLevel.valueOf(loginUser.getAccessLevel());
        if (loginLevel.equals(AccessLevel.EXTERNAL_USER) || loginLevel.equals(AccessLevel.EXTERNAL_ADMIN)) {
            return UserFilter.EXTERNAL_FILTER;
        } else {
            return UserFilter.TRIVIAL_FILTER;
        }
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, User loginUser) {
        List<GlobalAuthTeam> globalAuthTeams = globalTeamManagementService.getTeamsByUserName(username, true);
        return getGlobalTeams(globalAuthTeams, true, loginUser);
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
    public Boolean editTeam(String teamId, GlobalTeamData globalTeamData) {
        globalTeamManagementService.updateTeam(teamId, globalTeamData);
        return true;
    }

    @Override
    public Boolean editTeam(String teamId, UpdateTeamUsersRequest updateTeamUsersRequest) {
        globalTeamManagementService.updateTeam(teamId, updateTeamUsersRequest);
        return true;
    }

    @Override
    public Boolean deleteTeam(String teamId) {
        globalTeamManagementService.deleteTeamByTeamId(teamId);
        return true;
    }

    @Override
    public void deleteTeamByTenantId() {
        globalTeamManagementService.deleteTeamByTenantId();
    }

}
