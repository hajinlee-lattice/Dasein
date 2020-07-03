package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.auth.TeamEntities;

public interface TeamProxy {

    TeamEntities getTeamEntities(String customerSpace);

}
