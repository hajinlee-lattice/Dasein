package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

import static org.testng.Assert.assertEquals;

public class HDFSAccessMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private HDFSAccessMBean hdfsAcMBean;

    @Test(groups = "functional")
    public void testCheckHDFSStatus() {
        assertEquals("HDFS is accessible to dataplatform.", hdfsAcMBean.checkHDFSAccess());
    }
}
