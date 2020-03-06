package com.latticeengines.auth.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

public interface GlobalAuthTeamDao extends BaseDao<GlobalAuthTeam> {

    GlobalAuthTeam findByTeamNameAndTenantId(Long tenantId, String teamName);

    GlobalAuthTeam findByTeamIdAndTenantId(Long tenantId, String teamId);

}
