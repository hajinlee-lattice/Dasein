package com.latticeengines.playmaker.entitymgr.impl;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@ContextConfiguration(locations = { "classpath:test-playmaker-context.xml" })
public class PlaymakerTenantEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PlaymakerTenantEntityMgr playMakerEntityMgr;

    @Autowired
    private OAuthUserEntityMgr users;

    @Test(groups = "functional")
    public void testCRUD() throws Exception {

        PlaymakerTenant tenant = getTenant();
        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {

        }
        PlaymakerTenant result = playMakerEntityMgr.create(tenant);

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getTenantPassword());

        OAuthUser user = users.get(tenant.getTenantName());
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUserId(), tenant.getTenantName());
        Assert.assertNotNull(user.getPasswordExpiration());
        Assert.assertTrue(user.getPasswordExpiration().after(DateTime.now(DateTimeZone.UTC).toDate()));

        tenant.setExternalId("externalId2");
        playMakerEntityMgr.executeUpdate(tenant);
        result = playMakerEntityMgr.findByTenantName("playmaker");
        Assert.assertNotNull(result);

        playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        result = playMakerEntityMgr.findByTenantName("playmaker");
        Assert.assertNull(result);

        user = users.get(user.getUserId());
        Assert.assertNull(user);

        tenant = getTenant();
        result = playMakerEntityMgr.create(tenant);
    }

    @Test(groups = "functional")
    public void testPasswordExpiration() {
        PlaymakerTenant tenant = getTenant();
        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {

        }
        PlaymakerTenant result = playMakerEntityMgr.create(tenant);

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getTenantPassword());

        OAuthUser user = users.get(tenant.getTenantName());
        Assert.assertNotNull(user);
        Assert.assertFalse(user.getPasswordExpired());

        user.setPasswordExpired(true);
        users.update(user);

        user = users.get(user.getUserId());
        Assert.assertTrue(user.getPasswordExpired());

        tenant = getTenant();
        result = playMakerEntityMgr.create(tenant);
        user = users.get(user.getUserId());
        Assert.assertFalse(user.getPasswordExpired());
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
