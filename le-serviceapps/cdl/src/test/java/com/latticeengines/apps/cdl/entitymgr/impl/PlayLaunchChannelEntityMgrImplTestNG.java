package com.latticeengines.apps.cdl.entitymgr.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;

public class PlayLaunchChannelEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchEntityMgrImplTestNG.class);

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();

    }

    @Test(groups = "functional")
    public void testGetPreCreate() {

    }
}
