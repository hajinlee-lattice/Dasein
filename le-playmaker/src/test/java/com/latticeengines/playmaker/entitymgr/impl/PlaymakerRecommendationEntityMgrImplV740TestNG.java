package com.latticeengines.playmaker.entitymgr.impl;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public class PlaymakerRecommendationEntityMgrImplV740TestNG extends PlaymakerRecommendationEntityMgrImplTestNG {

    @Override
    public PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.118;instanceName=SQL2012STD;databaseName=PlayMakerDB740");
        tenant.setJdbcUserName("playmaker");
        tenant.setJdbcPassword("playmaker");
        tenant.setTenantName(getTenantName());
        return tenant;
    }
}
