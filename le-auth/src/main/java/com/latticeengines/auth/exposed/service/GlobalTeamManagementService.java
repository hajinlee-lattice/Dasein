package com.latticeengines.auth.exposed.service;

import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

public interface GlobalTeamManagementService {

    void createGlobalAuthTeam(GlobalAuthTeam globalAuthTeam);

    void updateGlobalAuthTeam(GlobalAuthTeam globalAuthTeam);

}
