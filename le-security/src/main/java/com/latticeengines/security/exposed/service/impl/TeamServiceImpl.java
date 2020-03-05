package com.latticeengines.security.exposed.service.impl;

import java.util.List;

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

        return null;
    }
}
