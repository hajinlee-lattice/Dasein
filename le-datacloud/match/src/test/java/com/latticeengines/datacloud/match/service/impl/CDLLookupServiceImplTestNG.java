package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class CDLLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CDLLookupServiceImplTestNG.class);

    @Inject
    private CDLLookupService cdlLookupService;

    @Inject
    private FabricDataService dataService;

    @Inject
    private DynamoItemService dynamoItemService;

    @BeforeClass(groups = "functional")
    public void setup() {
        // Add few records of data in dynamo
        // register a data unit
    }

//    @Test(groups = "manual", enabled = false)
//    public void testLookupInternalAccountId() {
//        DynamoDataUnit du = new DynamoDataUnit();
//        du.setSignature("20180425");
//        du.setTenant("LETest1590612472260");
//        du.setName("testtable");
//        String s = ((CDLLookupServiceImpl) cdlLookupService).lookupInternalAccountId(du, null, "60qbq7b2sb2gq6or");
//    }

    @Test(groups = "functional", enabled = false)
    public void testlookupContactsByInternalAccountId() {
        List<Map<String, Object>> contacts = cdlLookupService
                .lookupContactsByInternalAccountId("QA_CDL_DemoScript_1202", null, null, "0012400001DNrwwAAD", null);
        Assert.assertNotNull(contacts);
        Assert.assertEquals(contacts.size(), 25);
    }

    @Test(groups = "functional")
    public void testMerge() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream tableRegistryStream = classLoader.getResourceAsStream("cdl/ConsolidatedContact.json");
        String contactData = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());

        tableRegistryStream = classLoader.getResourceAsStream("cdl/CuratedContact.json");
        String curatedContactData = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());

        List<Map<String, Object>> merged = ((CDLLookupServiceImpl) cdlLookupService).merge(
                JsonUtils.convertListOfMaps(JsonUtils.deserialize(contactData, List.class), String.class, Object.class),
                JsonUtils.convertListOfMaps(JsonUtils.deserialize(curatedContactData, List.class), String.class,
                        Object.class));
        merged.forEach(m -> Assert.assertTrue(m.containsKey(InterfaceName.LastActivityDate.name())));
    }

    @AfterClass(groups = "functional", enabled = false)
    public void teardown() {
        // delete added records
        // deregister the data unit
    }
}
