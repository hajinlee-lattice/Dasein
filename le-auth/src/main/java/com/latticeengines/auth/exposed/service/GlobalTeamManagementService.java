package com.latticeengines.auth.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalTeam;

public interface GlobalTeamManagementService {

    void createTeam(GlobalTeam globalTeam);

    void updateTeam(GlobalTeam globalTeam);

    List<GlobalTeam> getTeams(boolean withTenantMember);

    List<GlobalTeam> getTeamsByUserName(String username, boolean withTenantMember);

}
