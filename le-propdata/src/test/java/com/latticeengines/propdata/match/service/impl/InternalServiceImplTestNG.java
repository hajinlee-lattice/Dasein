package com.latticeengines.propdata.match.service.impl;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.propdata.match.service.InternalService;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

public class InternalServiceImplTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    private InternalService internalService;

    @Test(groups = "functional")
    public void testCacheTableCreatedTime() {
        Date createdTime = internalService.currentCacheTableCreatedTime();
        Assert.assertNotNull(createdTime);
        Assert.assertTrue(createdTime.before(new Date()), "Crated time must be before now.");
    }

}
