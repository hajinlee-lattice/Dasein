package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

import static org.testng.Assert.assertTrue;

public class DataplatformCheckMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private DataplatformCheckMBean dataplatformCheckMBean;

    @Test(groups = "functional", enabled = true)
    public void testCheckDataplatform() {
        assertTrue(dataplatformCheckMBean.checkDataplatform().contains("passed"));
    }
}
