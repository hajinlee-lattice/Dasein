package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.auth.testframework.AuthFunctionalTestNGBase;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public class GlobalAuthUserTenantRightEntityMgrImplTestNG extends AuthFunctionalTestNGBase {

    @Autowired
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Test(groups = "functional")
    public void testCRUD() throws InterruptedException {
        GlobalAuthUser user = createGlobalAuthUser();
        Thread.sleep(200);
        user = gaUserEntityMgr.findByField("First_Name", user.getFirstName());
        Assert.assertNotNull(user);

        GlobalAuthTenant tenant = createGlobalAuthTenant();
        Thread.sleep(200);
        Assert.assertNotNull(tenant);

        tenant = gaTenantEntityMgr.findByTenantId(tenant.getId());


        GlobalAuthUserTenantRight tenantRight = new GlobalAuthUserTenantRight();
        tenantRight.setGlobalAuthTenant(tenant);
        tenantRight.setGlobalAuthUser(user);
        tenantRight.setOperationName("EXTERNAL_USER");
        globalAuthUserTenantRightEntityMgr.create(tenantRight);

        GlobalAuthUserTenantRight tenantRight2 = new GlobalAuthUserTenantRight();
        tenantRight2.setGlobalAuthTenant(tenant);
        tenantRight2.setGlobalAuthUser(user);
        tenantRight2.setOperationName("EXTERNAL_USER");
        tenantRight2.setExpirationDate(System.currentTimeMillis());
        globalAuthUserTenantRightEntityMgr.create(tenantRight2);

        List<GlobalAuthUserTenantRight> rights = globalAuthUserTenantRightEntityMgr.findByNonNullExprationDate();
        Assert.assertEquals(rights.size() >= 1, true);
        for (GlobalAuthUserTenantRight right : rights) {
            Assert.assertNotNull(right.getExpirationDate());
            Assert.assertNotNull(right.getGlobalAuthUser());
            Assert.assertNotNull(right.getGlobalAuthTenant());
        }

        rights = globalAuthUserTenantRightEntityMgr.findByUserIdAndTenantId(user.getPid(), tenant.getPid());
        Assert.assertEquals(rights.size(), 2);
        Boolean delete = globalAuthUserTenantRightEntityMgr.deleteByUserId(user.getPid());
        Assert.assertTrue(delete);
        rights = globalAuthUserTenantRightEntityMgr.findByUserIdAndTenantId(user.getPid(), tenant.getPid());
        Assert.assertNull(rights);
        gaUserEntityMgr.delete(user);
        gaTenantEntityMgr.delete(tenant);


    }

}
