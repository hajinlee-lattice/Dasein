package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

public interface GlobalAuthTeamEntityMgr extends BaseEntityMgr<GlobalAuthTeam> {

    List<GlobalAuthTeam> findByTenantId(Long tenantId);

    List<GlobalAuthTeam> findByTenantId(Long tenantId, boolean inflate);

    List<GlobalAuthTeam> findByUsernameAndTenantId(Long tenantId, String username, boolean inflate);

    List<GlobalAuthTeam> findByTeamIdsAndTenantId(Long tenantId, List<String> teamIds, boolean inflate);

    GlobalAuthTeam findByTeamNameAndTenantId(Long tenantId, String teamName);

    GlobalAuthTeam findByTeamIdAndTenantId(Long tenantId, String teamId);

    void deleteByTeamId(String teamId, Long tenantId);

    GlobalAuthTeam findByTeamIdAndTenantId(Long tenantId, String teamId, boolean inflate);

    boolean userBelongsToTeam(Long tenantId, String username, String teamId);
}
