package com.latticeengines.upgrade.yarn;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class YarnManagerTestNG extends UpgradeFunctionalTestNGBase {

    private final static String TEST_CUSTOMER = "Lattice_Relaunch";
    private final static String TEST_MODEL_GUID = "ms__b99ddcc6-7ecb-45a0-b128-9664b51c1ce9-PLSModel";

    @Autowired
    private YarnManager yarnManager;

    @Test(groups = "functional")
    public void testFindModelPath() throws Exception {
    }

}


