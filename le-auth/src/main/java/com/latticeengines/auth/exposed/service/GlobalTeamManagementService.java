package com.latticeengines.auth.exposed.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.auth.GlobalTeam;

public interface GlobalTeamManagementService {

    void createTeam(GlobalTeam globalTeam);

    void updateTeam(String teamId, String teamName, Set<String> teamMembers);

    List<GlobalTeam> getTeams(boolean withTenantMember);

    List<GlobalTeam> getTeamsByUserName(String username, boolean withTenantMember);

    GlobalTeam getTeamById(String teamId, boolean withTeamMember);

    Boolean deleteTeam(String teamId);
}
