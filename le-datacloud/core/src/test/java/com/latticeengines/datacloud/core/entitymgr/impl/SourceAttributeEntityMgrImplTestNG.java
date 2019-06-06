package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.VersionComparisonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;

public class SourceAttributeEntityMgrImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Autowired
    private SourceAttributeEntityMgr sourceAttributeEntityMgr;

    @Test(groups = "functional")
    public void testSourceAttributes() {
        DataCloudVersion version = dataCloudVersionEntityMgr.currentApprovedVersion();
        List<SourceAttribute> saList = sourceAttributeEntityMgr.getAttributes("AMProfile", "ENRICH",
                "SourceProfiler", version.getVersion(), false);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saList));
        saList = sourceAttributeEntityMgr.getAttributes("AMProfile", "SEGMENT", "SourceProfiler",
                version.getVersion(), true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(saList));
    }

    @Test(groups = "functional")
    public void testLatestDataCloudVersion() {
        // computing actual max datacloud version value
        String maxDataCloudVersion = sourceAttributeEntityMgr.getLatestDataCloudVersion(
                "AccountMaster", "CLEAN", "AMCleaner");
        Assert.assertTrue(StringUtils.isNotEmpty(maxDataCloudVersion));
        // computing the expected max datacloud version value
        List<String> versions = sourceAttributeEntityMgr
                .getAllDataCloudVersions("AccountMaster", "CLEAN", "AMCleaner");
        String maxVersion = Collections.max(versions, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return VersionComparisonUtils.compareVersion(o1, o2);
            }
        });
        // verifying actual and expected values computed above
        Assert.assertEquals(maxDataCloudVersion, maxVersion);

    }
}
