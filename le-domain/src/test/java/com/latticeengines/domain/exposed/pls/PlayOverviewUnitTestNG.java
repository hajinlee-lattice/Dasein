package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class PlayOverviewUnitTestNG {

    private final static String DISPLAY_NAME = "playHarder";
    private final static String DESCRIPTION = "playHardest";
    private final static String SEGMENT_NAME = "segment";
    private final static String CREATED_BY = "lattice@lattice-engines.com";
    private final static String TALKING_POINT_TITILE = "Reason to Buy";

    private final static Logger log = LoggerFactory.getLogger(PlayOverviewUnitTestNG.class);

    @Test(groups = "unit")
    public void testDeserialization() throws JsonProcessingException {
        PlayOverview playOverview = new PlayOverview();
        Play play = new Play();
        play.setTimestamp(new Date(System.currentTimeMillis()));
        play.setLastUpdatedTimestamp(new Date(System.currentTimeMillis()));
        play.setDisplayName(DISPLAY_NAME);
        play.setDescription(DESCRIPTION);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
        playOverview.setPlay(play);

        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setLastAccountsNum(100L);
        launchHistory.setLastContactsNum(200L);
        launchHistory.setNewAccountsNum(300L);
        launchHistory.setNewContactsNum(400L);
        playOverview.setLaunchHistory(launchHistory);

        Map<BucketName, Integer> accountRatingMap = new HashMap<BucketName, Integer>();
        accountRatingMap.put(BucketName.A, 500);
        accountRatingMap.put(BucketName.B, 1000);
        accountRatingMap.put(BucketName.C, 1000);
        accountRatingMap.put(BucketName.D, 1000);
        accountRatingMap.put(BucketName.F, 500);
        playOverview.setAccountRatingMap(accountRatingMap);

        List<TalkingPointDTO> talkingPoints = new ArrayList<TalkingPointDTO>();
        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setTitle(TALKING_POINT_TITILE);
        talkingPoints.add(tp);
        playOverview.setTalkingPoints(talkingPoints);

        log.info("playOverview is " + playOverview);
    }

}
