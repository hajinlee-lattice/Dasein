package com.latticeengines.playmaker.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerEntityMgr;

@ContextConfiguration(locations = { "classpath:playmaker-context.xml", "classpath:playmaker-properties-context.xml" })
public class PlaymakerEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PlaymakerEntityMgr playMakerEntityMgr;

    @Test(groups = "functional", enabled = true)
    public void testCRUD() throws Exception {

        PlaymakerTenant tenant = getTennat();
        playMakerEntityMgr.create(tenant);
        tenant.setTenantName("tenantName2");
        playMakerEntityMgr.executeUpdate(tenant);
        PlaymakerTenant tenant2 = playMakerEntityMgr.findByTenantName("tenantName2");
        Assert.assertNotNull(tenant2);

        playMakerEntityMgr.delete(tenant);

    }

    public static PlaymakerTenant getTennat() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcPassword("jdbcPassword");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.250:1437;databaseName=playmaker_dev");
        tenant.setJdbcUserName("jdbcUserName");
        tenant.setTenantName("tenantName");
        return tenant;
    }

}
