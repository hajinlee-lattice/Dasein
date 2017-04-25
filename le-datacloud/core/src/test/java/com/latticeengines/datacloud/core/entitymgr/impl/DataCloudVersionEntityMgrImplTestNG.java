package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

public class DataCloudVersionEntityMgrImplTestNG extends DataCloudCoreFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(DataCloudVersionEntityMgrImplTestNG.class);

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
        DataCloudVersion version = dataCloudVersionEntityMgr.latestApprovedForMajorVersion("98.0.12345");
        Assert.assertNotNull(version);
        Assert.assertEquals(version.getMajorVersion(), "98.0");
        Assert.assertEquals(version.getVersion(), "98.0.1");

        version = dataCloudVersionEntityMgr.latestApprovedForMajorVersion("98.0");
        Assert.assertNotNull(version);
        Assert.assertEquals(version.getMajorVersion(), "98.0");
        Assert.assertEquals(version.getVersion(), "98.0.1");
    }

    private List<DataCloudVersion> prepareVersions() {
        DataCloudVersion version1 = new DataCloudVersion();
        version1.setVersion("98.0.0");
        version1.setCreateDate(new Date());
        version1.setAccountMasterHdfsVersion("version1");
        version1.setAccountLookupHdfsVersion("version1");
        version1.setMajorVersion("98.0");
        version1.setStatus(DataCloudVersion.Status.APPROVED);
        version1.setMetadataRefreshDate(new Date());

        DataCloudVersion version2 = new DataCloudVersion();
        version2.setVersion("98.0.1");
        version2.setCreateDate(new Date());
        version2.setAccountMasterHdfsVersion("version2");
        version2.setAccountLookupHdfsVersion("version2");
        version2.setMajorVersion("98.0");
        version2.setStatus(DataCloudVersion.Status.APPROVED);
        version2.setMetadataRefreshDate(new Date());

        return Arrays.asList(version1, version2);
    }

    @Test(groups = "functional")
    public void testMetadataRefreshDate() {
        /*
         * Testing all versions and latest approved version if they have MetadataRefreshDate column
         * populated
         */
        // for latest approved version
        DataCloudVersion latestApprovedVersion = dataCloudVersionEntityMgr.latestApprovedForMajorVersion("2.0");
        log.info("latestApprovedVersion : " + latestApprovedVersion.getVersion());
        Assert.assertNotNull(latestApprovedVersion.getVersion());
        Date refreshDate = latestApprovedVersion.getMetadataRefreshDate();
        log.info("refresh date : " + refreshDate);
        Assert.assertNotNull(refreshDate);
        // for all versions
        List<DataCloudVersion> versions = dataCloudVersionEntityMgr.allVerions();
        for (DataCloudVersion version : versions) {
            log.info("version : " + version.getVersion());
            Assert.assertNotNull(version);
            if (version.getMajorVersion().equals("2.0")) {
                Date metadataRefreshDate = version.getMetadataRefreshDate();
                log.info("date : " + metadataRefreshDate);
                Assert.assertNotNull(metadataRefreshDate);
            }
        }
    }

    @Test(groups = "functional")
    public void testAllApprovedMajorVersions() {
        List<String> allApprovedMajorVersions = dataCloudVersionEntityMgr.allApprovedMajorVersions();
        Assert.assertNotNull(allApprovedMajorVersions);
        Assert.assertNotEquals(allApprovedMajorVersions.size(), 0);
        for (String majorVersion : allApprovedMajorVersions) {
            log.info("Found major version: " + majorVersion);
            Assert.assertTrue(StringUtils.isNotEmpty(majorVersion));
        }
    }
}
