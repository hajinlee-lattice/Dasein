package com.latticeengines.pls.mbean;

import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
public class HDFSAccessMBeanTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Inject
    private HDFSAccessMBean plsHdfsAcMBean;

    @Test(groups = {"functional", "functional.production"})
    public void testCheckHDFSStatus() {
        assertTrue(plsHdfsAcMBean.checkHDFSAccess().contains("SUCCESS"));
    }
}
