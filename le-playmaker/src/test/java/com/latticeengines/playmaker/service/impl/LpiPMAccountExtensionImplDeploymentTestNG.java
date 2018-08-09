package com.latticeengines.playmaker.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { //"classpath:test-pls-context.xml", 
		"classpath:test-testframework-cleanup-context.xml", 
		"classpath:playmakercore-context.xml",
        "classpath:test-playmaker-context.xml" })
public class LpiPMAccountExtensionImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(LpiPMAccountExtensionImplDeploymentTestNG.class);

    private LpiPMAccountExtensionImpl lpiPMAccountExtensionImpl;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private EntityQueryGenerator entityQueryGenerator;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    private long accountCount;

    List<String> accountFields = Arrays.asList( //
            "LatticeAccountId", //
            "CDLUpdatedTime", //
            "AccountId", //
            "Website", //
            "LDC_Name", //
            "ID", //
            "LEAccountExternalID", //
            "LastModificationDate", //
            "SalesforceAccountID", //
            "RowNum");

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();
        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        lpiPMAccountExtensionImpl = new LpiPMAccountExtensionImpl();
        lpiPMAccountExtensionImpl.setEntityProxy(entityProxy);
        lpiPMAccountExtensionImpl.setEntityQueryGenerator(entityQueryGenerator);
        lpiPMAccountExtensionImpl.setLookupIdMappingProxy(lookupIdMappingProxy);
    }

    @Test(groups = "deployment")
    public void testGetAccountExtensionCount() {
        accountCount = lpiPMAccountExtensionImpl.getAccountExtensionCount(0L, null, 0L);
        Assert.assertTrue(accountCount > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetAccountExtensionCount" })
    public void testGetAccountExtensions() {
        long max = accountCount > 5L ? 5L : accountCount;
        List<Map<String, Object>> datapage = //
                lpiPMAccountExtensionImpl.getAccountExtensions( //
                        0, 0, max, null, null, //
                        String.join(",", accountFields), false, null);

        Assert.assertNotNull(datapage);
        Assert.assertEquals(datapage.size(), (int) max);

        datapage.stream() //
                .forEach(row -> {
                    if (row.keySet().size() != accountFields.size()) {
                        log.info(String.format("Expected fields: %s , Result fields: %s", //
                                JsonUtils.serialize(accountFields), //
                                JsonUtils.serialize(row.keySet())));
                        log.info("Account Ext Data: " + JsonUtils.serialize(row));
                    }

                    Assert.assertEquals(row.keySet().size(), accountFields.size());
                    accountFields.stream() //
                            .forEach(field -> {
                                if (!row.containsKey(field)) {
                                    log.info(String.format("Expected field: %s , Result fields: %s", //
                                            field, //
                                            JsonUtils.serialize(row.keySet())));
                                }

                                Assert.assertTrue(row.containsKey(field),
                                        String.format("row = %s, field = %s", JsonUtils.serialize(row), field));
                                if (field.equals(InterfaceName.AccountId.name())) {
                                    Assert.assertNotNull(row.get(field));
                                }
                                Assert.assertNotNull(
                                        row.get(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY));
                            });
                });
    }
}
