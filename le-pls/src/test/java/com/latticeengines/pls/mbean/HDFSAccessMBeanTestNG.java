package com.latticeengines.pls.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class HDFSAccessMBeanTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private HDFSAccessMBean plsHdfsAcMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testCheckHDFSStatus() {
        assertTrue(plsHdfsAcMBean.checkHDFSAccess().contains("SUCCESS"));
    }
}
