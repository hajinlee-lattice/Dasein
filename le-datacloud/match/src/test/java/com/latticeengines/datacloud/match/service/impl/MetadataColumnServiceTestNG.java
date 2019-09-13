package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

public class MetadataColumnServiceTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Inject
    private DataCloudVersionService dataCloudVersionService;

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> metadataColumnService;

    private String currentVersion;
    private String prevVersion;

    @BeforeClass(groups = { "functional" })
    public void setup() {
        currentVersion = dataCloudVersionEntityMgr.currentApprovedVersionAsString();
        prevVersion = dataCloudVersionService.priorVersions(currentVersion, 2).get(1);
    }

    @Test(groups = { "functional" }, priority = 1)
    public void testS3Publish() {
        // Use previous version to test metadata publish so that other
        // functional tests which need to load latest version of metadata are
        // not impacted
        long actualTotal = metadataColumnService.s3Publish(prevVersion);
        long expectedTotal = metadataColumnService.count(prevVersion);
        Assert.assertEquals(actualTotal, expectedTotal);
    }

    @Test(groups = { "functional" }, priority = 2, dataProvider = "DataCloudVersions")
    public void testMetadataRefresh(String dataCloudVersion) {
        List<AccountMasterColumn> actualColumns = metadataColumnService.getMetadataColumns(dataCloudVersion);
        Collections.sort(actualColumns, (col1, col2) -> col1.getAmColumnId().compareTo(col2.getAmColumnId()));
        List<AccountMasterColumn> expectedColumns = metadataColumnService.scan(dataCloudVersion, null, null)
                .sequential()
                .collectList().block();
        Collections.sort(expectedColumns, (col1, col2) -> col1.getAmColumnId().compareTo(col2.getAmColumnId()));
        Assert.assertEquals(actualColumns.size(), expectedColumns.size());
        for (int i = 0; i < actualColumns.size(); i++) {
            validateAMColumn(actualColumns.get(i), expectedColumns.get(i));
        }
    }

    @DataProvider(name = "DataCloudVersions")
    private Object[][] getDataCloudVersions() {
        return new Object[][] { //
                { currentVersion }, //
                { prevVersion } //
        };
    }

    // Check not-null explicitly to ensure certain columns which are required to
    // have value are properly populated in both DB and during
    // serialization/de-serialization in publication
    private void validateAMColumn(AccountMasterColumn actual, AccountMasterColumn expected) {
        Assert.assertNotNull(actual.getPid());
        Assert.assertEquals(actual.getPid(), expected.getPid());
        Assert.assertNotNull(actual.getAmColumnId());
        Assert.assertEquals(actual.getAmColumnId(), expected.getAmColumnId());
        Assert.assertNotNull(actual.getDataCloudVersion());
        Assert.assertEquals(actual.getDataCloudVersion(), expected.getDataCloudVersion());
        Assert.assertNotNull(actual.getDisplayName());
        Assert.assertEquals(actual.getDisplayName(), expected.getDisplayName());
        Assert.assertEquals(actual.getDescription(), expected.getDescription());
        Assert.assertNotNull(actual.getJavaClass());
        Assert.assertEquals(actual.getJavaClass(), expected.getJavaClass());
        Assert.assertNotNull(actual.getCategory());
        Assert.assertEquals(actual.getCategory(), expected.getCategory());
        Assert.assertNotNull(actual.getSubcategory());
        Assert.assertEquals(actual.getSubcategory(), expected.getSubcategory());
        Assert.assertNotNull(actual.getStatisticalType());
        Assert.assertEquals(actual.getStatisticalType(), expected.getStatisticalType());
        Assert.assertNotNull(actual.getFundamentalType());
        Assert.assertEquals(actual.getFundamentalType(), expected.getFundamentalType());
        Assert.assertNotNull(actual.getApprovedUsage());
        Assert.assertEquals(actual.getApprovedUsage(), expected.getApprovedUsage());
        Assert.assertEquals(actual.getGroups(), expected.getGroups());
        Assert.assertEquals(actual.isInternalEnrichment(), expected.isInternalEnrichment());
        Assert.assertEquals(actual.isPremium(), expected.isPremium());
        Assert.assertEquals(actual.getDiscretizationStrategy(), expected.getDiscretizationStrategy());
        Assert.assertEquals(actual.getDecodeStrategy(), expected.getDecodeStrategy());
        Assert.assertEquals(actual.isEol(), expected.isEol());
        Assert.assertEquals(actual.getDataLicense(), expected.getDataLicense());
        Assert.assertEquals(actual.getEolVersion(), expected.getEolVersion());
        Assert.assertNotNull(actual.getRefreshFrequency());
        Assert.assertEquals(actual.getRefreshFrequency(), expected.getRefreshFrequency());
    }

}
