package com.latticeengines.pls.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class HDFSAccessMBeanTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private HDFSAccessMBean plsHdfsAcMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testCheckHDFSStatus() {
        assertTrue(plsHdfsAcMBean.checkHDFSAccess().contains("SUCCESS"));
    }
}
