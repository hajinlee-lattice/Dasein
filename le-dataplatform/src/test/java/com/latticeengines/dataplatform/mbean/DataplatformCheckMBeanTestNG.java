package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class DataplatformCheckMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private DataplatformCheckMBean dataplatformCheckMBean;

    @Test(groups = { "functional.platform", "functional.production" }, enabled = true)
    public void testCheckDataplatform() {
        assertTrue(dataplatformCheckMBean.checkDataplatform().contains("passed"));
    }
}
