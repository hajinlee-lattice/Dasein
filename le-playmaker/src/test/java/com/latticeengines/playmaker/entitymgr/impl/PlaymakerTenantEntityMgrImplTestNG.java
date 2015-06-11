package com.latticeengines.playmaker.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@ContextConfiguration(locations = { "classpath:playmaker-context.xml", "classpath:playmaker-properties-context.xml" })
public class PlaymakerTenantEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PlaymakerTenantEntityMgr playMakerEntityMgr;

    @Test(groups = "functional", enabled = true)
    public void testCRUD() throws Exception {

        PlaymakerTenant tenant = getTennat();
        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {

        }
        playMakerEntityMgr.create(tenant);
        tenant.setExternalId("externalId2");
        playMakerEntityMgr.executeUpdate(tenant);
        PlaymakerTenant tenant2 = playMakerEntityMgr.findByTenantName("tenantName");
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
