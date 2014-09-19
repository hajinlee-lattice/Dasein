package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class QuartzJobMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private QuartzJobMBean quartzJobMBean;
    
    @Test(groups = "functional")
    public void testCheckQuartzJob() {
        assertTrue(!quartzJobMBean.checkDLQuartzJob().contains("NoSuchBeanDefinitionException"));
        assertTrue(!quartzJobMBean.checkQuartzJob().contains("NoSuchBeanDefinitionException"));
    }
}
