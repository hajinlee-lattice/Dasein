package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.auth.testframework.AuthFunctionalTestNGBase;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
public class GlobalAuthUserTenantRightEntityMgrImplTestNG extends AuthFunctionalTestNGBase {

    @Inject
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Test(groups = "functional")
    public void testCRUD() throws InterruptedException {
        GlobalAuthUser user = createGlobalAuthUser();
        Thread.sleep(200);
        user = gaUserEntityMgr.findByField("First_Name", user.getFirstName());
        Assert.assertNotNull(user);

        GlobalAuthUser user2 = createGlobalAuthUser();
        Thread.sleep(200);
        user2 = gaUserEntityMgr.findByField("First_Name", user2.getFirstName());
        Assert.assertNotNull(user2);

        GlobalAuthTenant tenant = createGlobalAuthTenant();
        Thread.sleep(200);
        Assert.assertNotNull(tenant);

        tenant = gaTenantEntityMgr.findByTenantId(tenant.getId());


        GlobalAuthUserTenantRight tenantRight = new GlobalAuthUserTenantRight();
        tenantRight.setGlobalAuthTenant(tenant);
        tenantRight.setGlobalAuthUser(user);
        tenantRight.setOperationName("EXTERNAL_USER");
        globalAuthUserTenantRightEntityMgr.create(tenantRight);

        // This won't success if using the same user, we have unique constraint.
        GlobalAuthUserTenantRight tenantRight2 = new GlobalAuthUserTenantRight();
        tenantRight2.setGlobalAuthTenant(tenant);
        tenantRight2.setGlobalAuthUser(user2);
        tenantRight2.setOperationName("EXTERNAL_USER");
        tenantRight2.setExpirationDate(System.currentTimeMillis());
        globalAuthUserTenantRightEntityMgr.create(tenantRight2);

        List<GlobalAuthUserTenantRight> rights = globalAuthUserTenantRightEntityMgr.findByNonNullExpirationDate();
        Assert.assertTrue(rights.size() >= 1);
        for (GlobalAuthUserTenantRight right : rights) {
            Assert.assertNotNull(right.getExpirationDate());
            Assert.assertNotNull(right.getGlobalAuthUser());
            Assert.assertNotNull(right.getGlobalAuthTenant());
        }

        GlobalAuthUserTenantRight right = globalAuthUserTenantRightEntityMgr.findByUserIdAndTenantId(user.getPid(), tenant.getPid());
        Assert.assertNotNull(right);
        Boolean delete = globalAuthUserTenantRightEntityMgr.deleteByUserId(user.getPid());
        Assert.assertTrue(delete);
        right = globalAuthUserTenantRightEntityMgr.findByUserIdAndTenantId(user.getPid(), tenant.getPid());
        Assert.assertNull(right);
        gaUserEntityMgr.delete(user);
        gaUserEntityMgr.delete(user2);
        gaTenantEntityMgr.delete(tenant);

    }

}
