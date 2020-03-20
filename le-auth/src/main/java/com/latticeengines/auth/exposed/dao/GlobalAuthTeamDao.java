package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

public interface GlobalAuthTeamDao extends BaseDao<GlobalAuthTeam> {

    GlobalAuthTeam findByTeamNameAndTenantId(Long tenantId, String teamName);

    GlobalAuthTeam findByTeamIdAndTenantId(Long tenantId, String teamId);

    void deleteByTeamId(String teamId, Long tenantId);

    List<GlobalAuthTeam> findByUsernameAndTenantId(Long tenantId, String username);

    List<GlobalAuthTeam> findByTeamIdsAndTenantId(Long tenantId, List<String> teamIds);

    void deleteByTenantId(Long tenantId);

    boolean userBelongsToTeam(Long tenantId, String username, String teamId);

}
