package com.latticeengines.playmaker.entitymgr.impl;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.playmaker.functionalframework.PlaymakerTestNGBase;

public class PlaymakerTenantEntityMgrImplTestNG extends PlaymakerTestNGBase {

    @Inject
    private PlaymakerTenantEntityMgr playMakerEntityMgr;

    @Inject
    private OAuthUserEntityMgr users;

    @Test(groups = "functional", enabled = false)
    public void testCRUD() {

        PlaymakerTenant tenant = getTenant();
        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
            logger.warn("Error during tenant deletion, ignored", ex);
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
        result = playMakerEntityMgr.findByTenantName(getTenantName());
        Assert.assertNotNull(result);

        playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        result = playMakerEntityMgr.findByTenantName(getTenantName());
        Assert.assertNull(result);

        user = users.get(user.getUserId());
        Assert.assertNull(user);

        tenant = getTenant();
        playMakerEntityMgr.create(tenant);
    }

    @Test(groups = "functional", enabled = false)
    public void testPasswordExpiration() {
        PlaymakerTenant tenant = getTenant();
        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
            logger.warn("Error during tenant deletion, ignored", ex);
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
        playMakerEntityMgr.create(tenant);
        user = users.get(user.getUserId());
        Assert.assertFalse(user.getPasswordExpired());
    }

}
