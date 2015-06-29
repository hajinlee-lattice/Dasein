package com.latticeengines.playmaker.entitymgr.impl;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2.common.service.ExtendedClientDetails;
import com.latticeengines.playmaker.dao.PlaymakerOauth2DbDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@ContextConfiguration(locations = { "classpath:test-playmaker-context.xml" })
public class PlaymakerTenantEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PlaymakerTenantEntityMgr playMakerEntityMgr;

    @Autowired
    private PlaymakerOauth2DbDao playmakerOauth2DbDao;

    @Test(groups = "functional", enabled = true)
    public void testCRUD() throws Exception {

        PlaymakerTenant tenant = getTenant();
        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {

        }
        PlaymakerTenant result = playMakerEntityMgr.create(tenant);

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getTenantPassword());

        ExtendedClientDetails clientDetails = playmakerOauth2DbDao.getClientByClientId(tenant.getTenantName());
        Assert.assertNotNull(clientDetails);
        Assert.assertEquals(clientDetails.getClientId(), tenant.getTenantName());
        Assert.assertTrue(clientDetails.getAuthorizedGrantTypes().contains("client_credentials"));
        Assert.assertNotNull(clientDetails.getClientSecretExpiration());
        Assert.assertTrue(clientDetails.getClientSecretExpiration().isAfter(DateTime.now(DateTimeZone.UTC)));

        tenant.setExternalId("externalId2");
        playMakerEntityMgr.executeUpdate(tenant);
        PlaymakerTenant tenant2 = playMakerEntityMgr.findByTenantName("playmaker");
        Assert.assertNotNull(tenant2);

        // playMakerEntityMgr.delete(tenant);

    }

    public static PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("playmaker");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcPassword("playmaker");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.82\\sql2012std;databaseName=ADEDTBDd70064747nA26263627n1");
        tenant.setJdbcUserName("playmaker");
        tenant.setTenantName("playmaker");
        return tenant;
    }
}
