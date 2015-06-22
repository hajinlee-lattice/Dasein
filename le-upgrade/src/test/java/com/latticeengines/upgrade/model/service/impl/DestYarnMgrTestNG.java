package com.latticeengines.upgrade.model.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;
import com.latticeengines.upgrade.yarn.DestYarnMgr;

public class DestYarnMgrTestNG extends UpgradeFunctionalTestNGBase {

    private final static String TEST_CUSTOMER = "UpgradeTester";
    private final static String TEST_UUID = "ac427664-18ff-11e5-b60b-1697f925ec7b";

    @Autowired
    private DestYarnMgr yarnMgr;

    @Test(groups = "functional")
    public void testConstructModelDir() {
    }

}


