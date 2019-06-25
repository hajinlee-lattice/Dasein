package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public class PlayLaunchChannelEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchEntityMgrImplTestNG.class);

    @Autowired
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllScheduledChannels();
        Assert.assertNotNull(channels);
    }
}
