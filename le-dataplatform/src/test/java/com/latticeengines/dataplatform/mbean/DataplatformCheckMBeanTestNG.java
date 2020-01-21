package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
public class DataplatformCheckMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Inject
    private DataplatformCheckMBean dataplatformCheckMBean;

    @Test(groups = { "functional.platform", "functional.production" }, enabled = false)
    public void testCheckDataplatform() {
        assertTrue(dataplatformCheckMBean.checkDataplatform().contains("passed"));
    }
}
