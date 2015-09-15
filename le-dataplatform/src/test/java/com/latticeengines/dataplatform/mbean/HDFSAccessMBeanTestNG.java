package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class HDFSAccessMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private HDFSAccessMBean hdfsAcMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testCheckHDFSStatus() {
        assertEquals("HDFS is accessible to dataplatform.", hdfsAcMBean.checkHDFSAccess());
    }
}
