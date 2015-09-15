package com.latticeengines.dataplatform.mbean;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class HDFSResourceMBeanTestNG extends DataPlatformFunctionalTestNGBase {
    
    @Autowired
    private HDFSResourceMBean hdfsRcMBean;
    
    @Test(groups = {"functional", "functional.production"})
    public void testCheckHDFSResource() {
        String files = hdfsRcMBean.checkHDFSResource();
        assertTrue(files.contains("/app/dataplatform/dataplatform.properties"));
        assertTrue(files.contains("algorithm"));
        assertTrue(files.contains("launcher.py"));
        assertTrue(files.contains("leframework.tar.gz"));
    }
}
