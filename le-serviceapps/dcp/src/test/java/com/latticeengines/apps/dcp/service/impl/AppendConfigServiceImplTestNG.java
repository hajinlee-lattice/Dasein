package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

public class AppendConfigServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private AppendConfigServiceImpl appendConfigServiceImpl;

    @Test(groups = "functional")
    public void testGetEntitlement() {
        setupTestEnvironment();
        DataBlockEntitlementContainer container = //
                appendConfigServiceImpl.getSubscriberEntitlement("202007226");
        Assert.assertNotNull(container);
        container = appendConfigServiceImpl.getSubscriberEntitlement("123");
        Assert.assertNull(container);
    }

}
