package com.latticeengines.propdata.collection.entitymanager.impl;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.collection.entitymanager.HdfsSourceEntityMgr;
import com.latticeengines.propdata.collection.source.impl.CollectionSource;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;


@Component
public class HdfsSourceEntityMgrImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private final Source testSource = CollectionSource.FEATURE;

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {
        hdfsPathBuilder.changeHdfsPodId("FunctionalTestHdfsSource");
        HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.constructPodDir().toString());
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.constructPodDir().toString());
    }

    @Test(groups = "functional")
    public void testCurrentVersion() throws IOException {
        hdfsSourceEntityMgr.setCurrentVersion(testSource, "version1");
        Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(testSource), "version1");

        hdfsSourceEntityMgr.setCurrentVersion(testSource, "version2");
        Assert.assertEquals(hdfsSourceEntityMgr.getCurrentVersion(testSource), "version2");
    }

}
