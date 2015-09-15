package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

@ContextConfiguration(locations = { //
        "classpath:dataplatform-dlorchestration-quartz-context.xml", //
        "classpath:dataplatform-quartz-context.xml" })
public class QuartzJobMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private QuartzJobMBean quartzJobMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testCheckQuartzJob() {
        assertTrue(quartzJobMBean.checkDLQuartzJob().contains("enabled"));
        assertTrue(quartzJobMBean.checkQuartzJob().contains("enabled"));
    }
}
