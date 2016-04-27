package com.latticeengines.dataplatform.mbean;

import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class HDFSResourceMBeanTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private HDFSResourceMBean hdfsRcMBean;

    @Autowired
    private VersionManager versionManager;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Test(groups = { "functional", "functional.production" })
    public void testCheckHDFSResource() {
        String files = hdfsRcMBean.checkHDFSResource();
        String s = versionManager.getCurrentVersionInStack(stackName).equals("") ? "" : "/";
        assertTrue(files.contains("/app/" + versionManager.getCurrentVersionInStack(stackName) + s + "dataplatform/dataplatform.properties"));
        assertTrue(files.contains("algorithm"));
        assertTrue(files.contains("launcher.py"));
        assertTrue(files.contains("leframework.tar.gz"));
    }
}
