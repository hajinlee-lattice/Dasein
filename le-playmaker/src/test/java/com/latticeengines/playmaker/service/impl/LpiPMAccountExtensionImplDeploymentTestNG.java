package com.latticeengines.playmaker.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;
import com.latticeengines.pls.service.impl.TestPlayCreationHelper;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml", "classpath:playmakercore-context.xml",
        "classpath:test-playmaker-context.xml" })
public class LpiPMAccountExtensionImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(LpiPMAccountExtensionImplDeploymentTestNG.class);

    private LpiPMAccountExtensionImpl lpiPMAccountExtensionImpl;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    @Autowired
    private EntityQueryGenerator entityQueryGenerator;

    private Tenant tenant;

    private long accountCount;

    List<String> accountFields = Arrays.asList( //
            "LatticeAccountId", //
            "LastModifiedDate", //
            "AccountId", //
            "Website", //
            "LDC_Name", //
            "ID", //
            "LEAccountExternalID", //
            "LastModificationDate", //
            "RowNum");

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenant();
        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        tenant = testPlayCreationHelper.getTenant();

        MultiTenantContext.setTenant(tenant);

        lpiPMAccountExtensionImpl = new LpiPMAccountExtensionImpl();
        lpiPMAccountExtensionImpl.setEntityProxy(entityProxy);
        lpiPMAccountExtensionImpl.setEntityQueryGenerator(entityQueryGenerator);
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
                        String.join(",", accountFields), false);

        Assert.assertNotNull(datapage);
        Assert.assertEquals(datapage.size(), (int) max);

        datapage.stream() //
                .forEach(row -> {
                    if (row.keySet().size() != accountFields.size()) {
                        log.info(String.format("Expected fields: %s , Result fields: %s", //
                                JsonUtils.serialize(accountFields), //
                                JsonUtils.serialize(row.keySet())));
                    }

                    Assert.assertEquals(row.keySet().size(), accountFields.size());
                    accountFields.stream() //
                            .forEach(field -> {
                                if (!row.containsKey(field)) {
                                    log.info(String.format("Expected field: %s , Result fields: %s", //
                                            field, //
                                            JsonUtils.serialize(row.keySet())));
                                }

                                Assert.assertTrue(row.containsKey(field));
                                if (field.equals(InterfaceName.AccountId.name())) {
                                    Assert.assertNotNull(row.get(field));
                                }
                            });
                });
    }
}
