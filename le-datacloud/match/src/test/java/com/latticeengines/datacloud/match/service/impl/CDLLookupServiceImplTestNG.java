package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

public class CDLLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CDLLookupServiceImplTestNG.class);

    @Inject
    private CDLLookupService cdlLookupService;

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() {
        // Add few records of data in dynamo
        // register a data unit
    }

    @Test(groups = "functional", enabled = false)
    public void testlookupContactsByInternalAccountId() {
        List<Map<String, Object>> contacts = cdlLookupService
                .lookupContactsByInternalAccountId("QA_CDL_DemoScript_1202", null, null, "0012400001DNrwwAAD", null);
        Assert.assertNotNull(contacts);
        Assert.assertEquals(contacts.size(), 25);
    }

    @AfterClass(groups = "functional", enabled = false)
    public void teardown() {
        // delete added records
        // deregister the data unit
    }
}
