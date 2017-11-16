package com.latticeengines.dante.controller;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.entitymgr.DanteAccountEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.proxy.exposed.dante.DanteAccountProxy;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteAccountResourceDeploymentTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private DanteAccountProxy danteAccountProxy;

    @Autowired
    private DanteAccountEntityMgr danteAccountEntityMgr;

    private List<DanteAccount> accounts;
    private static final String customerId = "DEPTestTenant";

    @BeforeClass(groups = "deployment")
    public void setup() throws IOException {
        String accountsRaw = FileUtils.readFileToString(new File(
                ClassLoader.getSystemResource("com/latticeengines/dante/testframework/TestAccounts.json").getFile()),
                Charset.defaultCharset());
        accounts = JsonUtils.deserialize(accountsRaw, new TypeReference<List<DanteAccount>>() {
        });
        for (DanteAccount account : accounts) {
            danteAccountEntityMgr.create(account);
        }
    }

    @Test(groups = "deployment")
    public void testGetAccounts() {
        List<DanteAccount> result = danteAccountProxy.getAccounts(10, CustomerSpace.parse(customerId).toString());

        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 10);
    }

    @AfterClass(groups = "deployment")
    public void cleanup() {
        for (DanteAccount account : accounts) {
            danteAccountEntityMgr.delete(account);
        }
    }
}
