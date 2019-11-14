package com.latticeengines.app.exposed.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.aws.dynamo.DynamoItemService;

public class DataLakeServiceImplFunctionalTestNG extends AppFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DataLakeServiceImplFunctionalTestNG.class);

    @Inject
    private DataLakeService dataLakeService;

    @Inject
    private DynamoItemService dynamoItemService;

    @Test(groups = "functional")
    public void testGetInternalIdViaAccountCache() {
        ((DataLakeServiceImpl) dataLakeService).setAccountLookupCacheTable("AtlasLookupCache_20191107");
        Assert.assertEquals(((DataLakeServiceImpl) dataLakeService).getInternalIdViaAccountCache("Dell_NA_Atlas",
                "crmaccount_external_id", "0012g00001xeEMxqAb"), "123");
        Assert.assertEquals(((DataLakeServiceImpl) dataLakeService).getInternalIdViaAccountCache("Dell_NA_Atlas",
                "crmaccount_external_id", "0012g00001xeEMxqA1"), "123");
        Assert.assertEquals(((DataLakeServiceImpl) dataLakeService).getInternalIdViaAccountCache("Dell_NA_Atlas", null,
                "0012g00001xeEMxqA1"), "123");
        Assert.assertNull(((DataLakeServiceImpl) dataLakeService).getInternalIdViaAccountCache("Dell_NA_Atlas", "acco",
                "0012g00001xeEMxqA11"));
        Assert.assertNull(((DataLakeServiceImpl) dataLakeService).getInternalIdViaAccountCache("Dell_NA_Atlas", null,
                "0012g00001xeEMxqAs1"));
    }
}
