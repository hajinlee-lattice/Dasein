package com.latticeengines.security.exposed.service.impl;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.security.exposed.service.TeamService;

@Component("teamService")
public class TeamServiceImpl implements TeamService {

    private static final Logger log = LoggerFactory.getLogger(TeamServiceImpl.class);

    @Inject
    private GlobalTeamManagementService globalTeamManagementService;

    @Override
    public List<GlobalTeam> getTeams() {
        return globalTeamManagementService.getTeams(true);
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username) {
        return globalTeamManagementService.getTeamsByUserName(username, true);
    }

    @Override
    public GlobalTeam createTeam(String teamName, String createdByUser, Set<String> teamMembers) {
        GlobalTeam globalTeam = new GlobalTeam();
        globalTeam.setTeamMembers(teamMembers);
        globalTeam.setCreatedByUser(createdByUser);
        globalTeam.setTeamId(GlobalTeam.generateId());
        globalTeam.setTeamName(teamName);
        globalTeamManagementService.createTeam(globalTeam);
        return globalTeam;
    }
}
