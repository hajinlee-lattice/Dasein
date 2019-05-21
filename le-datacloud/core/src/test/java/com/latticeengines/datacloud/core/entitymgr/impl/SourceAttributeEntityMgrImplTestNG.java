package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.codehaus.plexus.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    public void testMaxDataCloudVersion() {
        String maxDataCloudVersion = sourceAttributeEntityMgr.getMaxDataCloudVersion(
                "AccountMaster", "CLEAN", "AMCleaner");
        Assert.assertTrue(StringUtils.isNotEmpty(maxDataCloudVersion));
        Assert.assertEquals(maxDataCloudVersion, "2.0.18");

    }
}
