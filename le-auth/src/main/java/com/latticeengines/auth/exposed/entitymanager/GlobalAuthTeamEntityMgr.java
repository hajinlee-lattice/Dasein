package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

public interface GlobalAuthTeamEntityMgr extends BaseEntityMgr<GlobalAuthTeam> {
    List<GlobalAuthTeam> findByTenantId(Long tenantId);
}
