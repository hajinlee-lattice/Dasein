package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.AccountEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteAccount;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class AccountEntityMgrTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private AccountEntityMgr accountCacheEntityMgr;

    @Test(groups = "functional")
    public void testGetAccounts() {
        List<DanteAccount> accounts = accountCacheEntityMgr.getAccounts(10);

        Assert.assertNotNull(accounts);
        Assert.assertEquals(accounts.size(), 10);
    }
}
