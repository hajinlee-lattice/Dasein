package com.latticeengines.security.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.auth.GlobalTeam;

public interface TeamService {

    List<GlobalTeam> getTeams();
}
