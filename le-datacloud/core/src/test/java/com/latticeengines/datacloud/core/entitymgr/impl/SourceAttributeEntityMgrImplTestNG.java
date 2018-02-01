package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
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
}
