package com.latticeengines.auth.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthSubscriptionEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.auth.testframework.AuthFunctionalTestNGBase;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public class GlobalAuthSubscriptionServiceImplTestNG extends AuthFunctionalTestNGBase {

    @Inject
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Inject
    private GlobalAuthSubscriptionService globalAuthSubscriptionService;

    @Inject
    private GlobalAuthSubscriptionEntityMgr globalAuthSubscriptionEntityMgr;

    private GlobalAuthUser user;
    private GlobalAuthTenant tenant;
    private GlobalAuthUserTenantRight tenantRight;
    private String email;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        user = createGlobalAuthUser();
        tenant = createGlobalAuthTenant();
        Thread.sleep(200);
        tenantRight = new GlobalAuthUserTenantRight();
        tenantRight.setGlobalAuthTenant(tenant);
        tenantRight.setGlobalAuthUser(user);
        tenantRight.setOperationName("EXTERNAL_USER");
        globalAuthUserTenantRightEntityMgr.create(tenantRight);
        email = user.getEmail();
    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        List<String> emailList = globalAuthSubscriptionService.getEmailsByTenantId(tenant.getId());
        if (CollectionUtils.isNotEmpty(emailList)) {
            globalAuthSubscriptionService.deleteByEmailAndTenantId(email, tenant.getId());
        }
        gaUserEntityMgr.delete(user);
        gaTenantEntityMgr.delete(tenant);
        globalAuthUserTenantRightEntityMgr.delete(tenantRight);
    }

    @Test(groups = "functional")
    public void testCRUD() {
        List<String> tenantIds = globalAuthSubscriptionService.getAllTenantId();
        Assert.assertTrue(!tenantIds.contains(tenant.getId()));
        List<String> emailList = globalAuthSubscriptionService.getEmailsByTenantId(tenant.getId());
        Assert.assertTrue(CollectionUtils.isEmpty(emailList));
        Assert.assertNull(globalAuthSubscriptionEntityMgr.findByUserTenantRight(tenantRight));

        Set<String> emailSet = new HashSet<>(Arrays.asList(email));
        emailList = globalAuthSubscriptionService.createByEmailsAndTenantId(emailSet, tenant.getId());
        Assert.assertTrue(emailsEqual(emailSet, emailList));
        Assert.assertTrue(emailsEqual(emailSet, globalAuthSubscriptionService.getEmailsByTenantId(tenant.getId())));
        tenantIds = globalAuthSubscriptionService.getAllTenantId();
        Assert.assertTrue(tenantIds.contains(tenant.getId()));
        Assert.assertNotNull(globalAuthSubscriptionEntityMgr.findByUserTenantRight(tenantRight));

        globalAuthSubscriptionService.deleteByEmailAndTenantId(email, tenant.getId());
        emailList = globalAuthSubscriptionService.getEmailsByTenantId(tenant.getId());
        Assert.assertTrue(CollectionUtils.isEmpty(emailList));
        tenantIds = globalAuthSubscriptionService.getAllTenantId();
        Assert.assertTrue(!tenantIds.contains(tenant.getId()));
    }

    private boolean emailsEqual(Set<String> emailSet, List<String> emailList) {
        if (emailSet == null) {
            return emailList == null ? true : false;
        } else if (emailSet.size() != emailList.size()) {
            return false;
        } else {
            for (String email : emailList) {
                if (!emailSet.contains(email)) {
                    return false;
                }
            }
            return true;
        }
    }
}
