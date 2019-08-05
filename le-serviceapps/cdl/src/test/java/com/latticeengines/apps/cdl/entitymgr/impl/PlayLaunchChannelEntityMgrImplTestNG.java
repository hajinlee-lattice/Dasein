package com.latticeengines.apps.cdl.entitymgr.impl;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.core.util.CronExpression;
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
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();
        Assert.assertNotNull(channels);
    }

    @Test(groups = "functional")
    public void testNextDateFromCronExpression() throws ParseException {
        CronExpression cronExp = new CronExpression("0 0 12 ? * WED *");
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setCronScheduleExpression("0 0 12 ? * WED *");

        Date d1 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        channel.setNextScheduledLaunch(
                Date.from(LocalDate.of(2019, 6, 14).atStartOfDay(ZoneId.systemDefault()).toInstant()));

        Date d2 = PlayLaunchChannel.getNextDateFromCronExpression(channel);

        Assert.assertEquals(d1, d2);
    }
}
