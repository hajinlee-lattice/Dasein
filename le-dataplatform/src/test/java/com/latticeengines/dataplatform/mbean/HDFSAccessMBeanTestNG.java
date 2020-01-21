package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
public class HDFSAccessMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Inject
    private HDFSAccessMBean hdfsAcMBean;

    @Test(groups = { "functional.platform", "functional.production" })
    public void testCheckHDFSStatus() {
        assertEquals("HDFS is accessible to dataplatform.", hdfsAcMBean.checkHDFSAccess());
    }
}
