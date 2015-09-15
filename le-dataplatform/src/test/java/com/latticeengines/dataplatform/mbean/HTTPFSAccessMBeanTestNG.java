package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:common-jmx-component-context.xml" })
public class HTTPFSAccessMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private HTTPFSAccessMBean httpFSMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testHttpFS() {
        assertTrue(httpFSMBean.checkHttpAccess().contains("\"type\":\"FILE\""));
    }
}
