package com.latticeengines.playmaker.entitymgr;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public interface PlaymakerTenantEntityMgr {

    void executeUpdate(PlaymakerTenant tenant);

    void create(PlaymakerTenant tenant);

    PlaymakerTenant findByKey(PlaymakerTenant tenant);

    void delete(PlaymakerTenant tenant);

    PlaymakerTenant findByTenantName(String tenantName);

    boolean deleteByTenantName(String tenantName);

    void updateByTenantName(PlaymakerTenant tenant);

}
