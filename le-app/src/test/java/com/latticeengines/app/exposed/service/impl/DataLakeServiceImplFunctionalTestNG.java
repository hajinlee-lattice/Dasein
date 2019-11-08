package com.latticeengines.app.exposed.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;

public class DataLakeServiceImplFunctionalTestNG extends AppFunctionalTestNGBase {

    @Inject
    private DataLakeService dataLakeService;

    @Test(groups = "functional")
    public void testGetInternalIdViaAccountCache() {
        ((DataLakeServiceImpl) dataLakeService).setAccountLookupCacheTable("AtlasLookupCache_20191107");
        Assert.assertEquals(((DataLakeServiceImpl) dataLakeService).getInternalIdViaAccountCache("Dell_NA_Atlas",
                "crmaccount_external_id", "0012g00001xeEMxqAb"), "123");
    }
}
