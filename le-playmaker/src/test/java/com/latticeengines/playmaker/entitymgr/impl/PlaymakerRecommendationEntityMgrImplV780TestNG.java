package com.latticeengines.playmaker.entitymgr.impl;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public class PlaymakerRecommendationEntityMgrImplV780TestNG extends PlaymakerRecommendationEntityMgrImplTestNG {

    @Override
    public PlaymakerTenant getTennat() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.118;instanceName=SQL2012STD;databaseName=PlayMakerDB780");
        tenant.setJdbcUserName("playmaker");
        tenant.setJdbcPassword("playmaker");
        tenant.setTenantName(PlaymakerTenantEntityMgrImplTestNG.getTenantName());
        return tenant;
    }
}
