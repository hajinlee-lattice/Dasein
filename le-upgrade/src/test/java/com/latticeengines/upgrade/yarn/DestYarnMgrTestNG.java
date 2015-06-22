package com.latticeengines.upgrade.yarn;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class DestYarnMgrTestNG extends UpgradeFunctionalTestNGBase {

    private final static String TEST_CUSTOMER = "UpgradeTester";
    private final static String TEST_UUID = "ac427664-18ff-11e5-b60b-1697f925ec7b";

    @Autowired
    private DestYarnMgr yarnMgr;

    @Test(groups = "functional")
    public void testConstructModelDir() {
    }

}


