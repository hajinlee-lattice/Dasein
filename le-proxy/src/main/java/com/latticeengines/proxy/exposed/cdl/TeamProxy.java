package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.auth.TeamEntityList;

public interface TeamProxy {

    TeamEntityList getTeamEntities(String customerSpace);

}
