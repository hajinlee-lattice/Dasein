package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ChannelSummaryUnitTestNG {

    private static final String PLAY_DISPLAY_NAME = "playName";
    private static final String PERIOD_STRING = "P3W";
    private static final String CREATED_BY = "userName";
    
    @Test(groups = "unit")
    public void testCreation() {
        Play play = new Play();
        Date date = new Date();

        play.setDisplayName(PLAY_DISPLAY_NAME);
        String playName = play.getName();
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setNextScheduledLaunch(date);
        channel.setExpirationPeriodString(PERIOD_STRING);
        channel.setUpdatedBy(CREATED_BY);
        channel.setPlay(play);
        ChannelSummary channelSummary = new ChannelSummary(channel);
        Assert.assertEquals(channelSummary.getPlayDisplayName(), PLAY_DISPLAY_NAME);
        Assert.assertEquals(channelSummary.getPlayName(), playName);
        Assert.assertEquals(channelSummary.getExpirationPeriodString(), PERIOD_STRING);
        Assert.assertEquals(channelSummary.getUpdatedBy(), CREATED_BY);
        Assert.assertEquals(channelSummary.getNextScheduledLaunch(), date);
    }
}
