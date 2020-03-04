package com.latticeengines.security.exposed.globalauth;

import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

public interface GlobalTeamManagementService {

    void createGlobalAuthTeam(GlobalAuthTeam globalAuthTeam);

    void updateGlobalAuthTeam(GlobalAuthTeam globalAuthTeam);

}
