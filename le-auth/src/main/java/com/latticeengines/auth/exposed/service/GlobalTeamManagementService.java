package com.latticeengines.auth.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;

public interface GlobalTeamManagementService {

    String createTeam(String createdByUser, GlobalTeamData globalTeamData);

    void updateTeam(String teamId, GlobalTeamData globalTeamData);

    List<GlobalAuthTeam> getTeams(boolean withTenantMember);

    List<GlobalAuthTeam> getTeamsByTeamIds(List<String> teamIds, boolean withTeamMember);

    List<GlobalAuthTeam> getTeamsByUserName(String username, boolean withTenantMember);

    GlobalAuthTeam getTeamById(String teamId, boolean withTeamMember);

    Boolean deleteTeamByTeamId(String teamId);

    boolean userBelongsToTeam(String username, String teamId);

}
