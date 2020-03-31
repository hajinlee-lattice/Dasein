package com.latticeengines.security.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.User;

public interface TeamService {

    List<GlobalTeam> getTeams(User loginUser);

    List<GlobalTeam> getTeamsByUserName(String username, User loginUser, boolean withTeamMember);

    List<GlobalTeam> getTeamsByUserName(String username, User loginUser);

    GlobalTeam getTeamByTeamId(String teamId, User loginUser);

    String createTeam(String createdByUser, GlobalTeamData globalTeamData);

    Boolean editTeam(User loginUser, String teamId, GlobalTeamData globalTeamData);

    Boolean editTeam(String teamId, GlobalTeamData globalTeamData);

    Boolean deleteTeam(String teamId);

    Map<String, List<String>> getDependencies(String teamId) throws Exception;

    boolean userBelongsToTeam(String username, String teamId);

    List<GlobalTeam> getTeamsInContext();

    GlobalTeam getTeamInContext(String teamId);

    GlobalTeam getTeamByTeamId(String teamId, User loginUser, boolean withTeamMember);
}
