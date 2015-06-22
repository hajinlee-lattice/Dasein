package com.latticeengines.upgrade.model.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.yarn.DestYarnMgr;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-upgrade-context.xml" })
public class DestYarnMgrTestNG {

    private final static String TEST_CUSTOMER = "UpgradeTester";
    private final static String TEST_UUID = "ac427664-18ff-11e5-b60b-1697f925ec7b";

    @Autowired
    private DestYarnMgr yarnMgr;

    @Test
    public void testConstructModelDir() {
    }

}


