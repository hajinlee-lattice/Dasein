package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.UpdateTeamUsersRequest;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.User;

public interface TeamService {

    List<GlobalTeam> getTeams(User loginUser);

    List<GlobalTeam> getTeamsByUserName(String username, User loginUser);

    String createTeam(String createdByUser, GlobalTeamData globalTeamData);

    Boolean editTeam(String teamId, GlobalTeamData globalTeamData);

    Boolean editTeam(String teamId, UpdateTeamUsersRequest updateTeamUsersRequest);

    Boolean deleteTeam(String teamId);
}
