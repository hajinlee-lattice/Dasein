package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

public class DataCloudVersionEntityMgrImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        teardown();

        List<DataCloudVersion> versions = prepareVersions();
        for (DataCloudVersion version: versions) {
            dataCloudVersionEntityMgr.createVersion(version);
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        List<DataCloudVersion> versions = prepareVersions();
        for (DataCloudVersion version: versions) {
            dataCloudVersionEntityMgr.deleteVersion(version.getVersion());
        }
    }

    @Test(groups = "functional")
    public void testLatestApproved() {
        DataCloudVersion version = dataCloudVersionEntityMgr.latestApprovedForMajorVersion("1.0.12345");
        Assert.assertNotNull(version);
        Assert.assertEquals(version.getMajorVersion(), "1.0");
        Assert.assertEquals(version.getVersion(), "1.0.0");

        version = dataCloudVersionEntityMgr.latestApprovedForMajorVersion("1.0");
        Assert.assertNotNull(version);
        Assert.assertEquals(version.getMajorVersion(), "1.0");
        Assert.assertEquals(version.getVersion(), "1.0.0");
    }

    private List<DataCloudVersion> prepareVersions() {
        DataCloudVersion version1 = new DataCloudVersion();
        version1.setVersion("1.0.0");
        version1.setCreateDate(new Date());
        version1.setAccountMasterHdfsVersion("version1");
        version1.setAccountLookupHdfsVersion("version1");
        version1.setMajorVersion("1.0");
        version1.setStatus(DataCloudVersion.Status.APPROVED);

        DataCloudVersion version2 = new DataCloudVersion();
        version2.setVersion("1.0.1");
        version2.setCreateDate(new Date());
        version2.setAccountMasterHdfsVersion("version2");
        version2.setAccountLookupHdfsVersion("version2");
        version2.setMajorVersion("1.0");
        version2.setStatus(DataCloudVersion.Status.APPROVED);

        return Arrays.asList(version1, version2);
    }
}
