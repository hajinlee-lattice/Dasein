package com.latticeengines.datacloud.core.entitymgr.impl;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.Feature;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.datacloud.core.util.HdfsPodContext;

@Component
public class HdfsSourceEntityMgrImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    Feature testSource;

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {
        HdfsPodContext.changeHdfsPodId("FunctionalTestHdfsSource");
        HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
    }

    @Test(groups = "functional")
    public void testCurrentVersion() throws IOException {
        hdfsSourceEntityMgr.setCurrentVersion(testSource, "version1");
        Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(testSource), "version1");

        hdfsSourceEntityMgr.setCurrentVersion(testSource, "version2");
        Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(testSource), "version2");
    }

}
