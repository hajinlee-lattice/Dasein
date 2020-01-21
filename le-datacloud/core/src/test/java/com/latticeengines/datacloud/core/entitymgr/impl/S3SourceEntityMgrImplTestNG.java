package com.latticeengines.datacloud.core.entitymgr.impl;

import java.io.IOException;

import javax.inject.Inject;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.S3SourceEntityMgr;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
public class S3SourceEntityMgrImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Inject
    private S3SourceEntityMgr s3SourceEntityMgr;

    private String sourceName = "HGDataClean";
    private String version = "2017-05-17_20-21-16_UTC";

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
    }

    @Test(groups = "functional", enabled = false)
    public void testDownload() throws IOException {
        s3SourceEntityMgr.downloadToHdfs(sourceName, version, "default");
    }

}
