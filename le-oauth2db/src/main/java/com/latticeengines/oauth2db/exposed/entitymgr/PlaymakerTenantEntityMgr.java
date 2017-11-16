package com.latticeengines.oauth2db.exposed.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public interface PlaymakerTenantEntityMgr {

    void executeUpdate(PlaymakerTenant tenant);

    PlaymakerTenant create(PlaymakerTenant tenant);

    PlaymakerTenant findByKey(PlaymakerTenant tenant);

    PlaymakerTenant findByTenantName(String tenantName);

    void deleteByTenantName(String tenantName);

    void updateByTenantName(PlaymakerTenant tenant);

    List<PlaymakerTenant> findAll();

}
