package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.DanteAccountEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteAccount;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteAccountEntityMgrTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private DanteAccountEntityMgr accountCacheEntityMgr;

    @Test(groups = "functional")
    public void testGetAccounts() {
        List<DanteAccount> accounts = accountCacheEntityMgr.getAccounts(10, "LECLEANX");

        Assert.assertNotNull(accounts);
        Assert.assertEquals(accounts.size(), 10);
    }
}
