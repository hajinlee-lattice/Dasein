package com.latticeengines.security.exposed.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.auth.GlobalTeam;

public interface TeamService {

    List<GlobalTeam> getTeams();

    List<GlobalTeam> getTeamsByUserName(String username);

    GlobalTeam createTeam(String teamName, String createdByUser, Set<String> teamMembers);
}
