package com.latticeengines.dante.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dante.service.AccountService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class AccountServiceImpleTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private AccountService accountService;

    @Test(groups = "functional")
    public void testGetAccountAttributes() {
        Map<String, String> accounts = accountService.getAccountAttributes();
        Assert.assertNotNull(accounts);
    }
}
