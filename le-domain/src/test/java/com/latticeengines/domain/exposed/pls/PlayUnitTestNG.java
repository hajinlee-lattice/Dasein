package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

public class PlayUnitTestNG {

    private static final String DISPLAY_NAME = "playHarder";
    private static final String DESCRIPTION = "playHardest";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String TALKING_POINT_TITLE = "Reason to Buy";
    private static final String TALKING_POINT_CONTENT = "Because we want your money";

    private static final Logger log = LoggerFactory.getLogger(PlayUnitTestNG.class);

    @Test(groups = "unit")
    public void testDeserialization() throws JsonProcessingException {

        Play play = new Play();
        play.setCreated(new Date(System.currentTimeMillis()));
        play.setUpdated(new Date(System.currentTimeMillis()));
        play.setDisplayName(DISPLAY_NAME);
        play.setDescription(DESCRIPTION);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);

        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setNewAccountsNum(300L);
        launchHistory.setNewContactsNum(400L);
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
        launchHistory.setLastIncompleteLaunch(playLaunch);
        play.setLaunchHistory(launchHistory);

        List<TalkingPoint> talkingPoints = new ArrayList<>();
        TalkingPoint tp = new TalkingPoint();
        tp.setTitle(TALKING_POINT_TITLE);
        tp.setContent(TALKING_POINT_CONTENT);
        talkingPoints.add(tp);
        play.setTalkingPoints(talkingPoints);

        log.info("playOverview is " + play);
    }

}
