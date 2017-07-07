package com.latticeengines.dante.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.proxy.exposed.dante.DanteAccountProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteAccountResourceDeploymentTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private DanteAccountProxy danteAccountProxy;

    @Test(groups = "deployment")
    public void testGetAccounts() {
        // Todo: create test tenant, add accounts in dante and then test this
        ResponseDocument<List<DanteAccount>> result = danteAccountProxy.getAccounts(10, "LECLEANX.LECLEANX.Production");

        Assert.assertNotNull(result);
        Assert.assertNull(result.getErrors());
        Assert.assertEquals(result.getResult().size(), 10);
    }
}
