package com.latticeengines.propdata.api.controller;

import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.propdata.InternalProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;

public class InternalResourceTestNG extends PropDataApiFunctionalTestNGBase {

    @Autowired
    private InternalProxy internalProxy;

    @Test(groups = "api")
    public void testCacheTableVersion() {
        Date createdTime = internalProxy.currentCacheTableVersion();
        Assert.assertNotNull(createdTime);
        Assert.assertTrue(createdTime.before(new Date()), "Crated time must be before now.");
    }

}
