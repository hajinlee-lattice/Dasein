package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

public class EntitlementServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private EntitlementServiceImpl entitlementServiceImpl;

    @Test(groups = "functional")
    public void testGetEntitlement() {
        setupTestEnvironment();
        DataBlockEntitlementContainer container = //
                entitlementServiceImpl.getSubscriberEntitlement(SUBSRIBER_NUMBER_SNMS);
        Assert.assertNotNull(container);
        container = entitlementServiceImpl.getSubscriberEntitlement("123");
        Assert.assertNull(container);
    }

}
