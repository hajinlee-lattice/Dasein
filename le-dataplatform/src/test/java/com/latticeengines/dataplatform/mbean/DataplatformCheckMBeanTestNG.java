package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

@ContextConfiguration(locations = { //
        "classpath:dataplatform-dlorchestration-quartz-context.xml", //
        "classpath:dataplatform-quartz-context.xml", //
        "classpath:common-jmx-component-context.xml" //
        })
public class DataplatformCheckMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private DataplatformCheckMBean dataplatformCheckMBean;

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testCheckDataplatform() {
        assertTrue(dataplatformCheckMBean.checkDataplatform().contains("passed"));
    }
}
