package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class HTTPFSAccessMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private HTTPFSAccessMBean httpFSMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testHttpFS() {
        assertTrue(httpFSMBean.checkHttpAccess().contains("\"type\":\"FILE\""));
    }
}
