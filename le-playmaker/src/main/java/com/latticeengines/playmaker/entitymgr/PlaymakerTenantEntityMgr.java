package com.latticeengines.playmaker.entitymgr;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public interface PlaymakerTenantEntityMgr {

    public static final String TENANT_NAME_KEY = "tenant_name";
    public static final String TENANT_PASSWORD_KEY = "tenant_password";

    void executeUpdate(PlaymakerTenant tenant);

    PlaymakerTenant create(PlaymakerTenant tenant);

    PlaymakerTenant findByKey(PlaymakerTenant tenant);

    void delete(PlaymakerTenant tenant);

    PlaymakerTenant findByTenantName(String tenantName);

    boolean deleteByTenantName(String tenantName);

    void updateByTenantName(PlaymakerTenant tenant);

    String findTenantByTokenId(String tokenId);

}
