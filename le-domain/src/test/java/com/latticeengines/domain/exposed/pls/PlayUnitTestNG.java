package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class PlayUnitTestNG {

    private final static String DISPLAY_NAME = "playHarder";
    private final static String DESCRIPTION = "playHardest";
    private final static String SEGMENT_NAME = "segment";
    private final static String CREATED_BY = "lattice@lattice-engines.com";
    private final static String TALKING_POINT_TITLE = "Reason to Buy";
    private final static String TALKING_POINT_CONTENT = "Because we want your money";

    private final static Logger log = LoggerFactory.getLogger(PlayUnitTestNG.class);

    @Test(groups = "unit")
    public void testDeserialization() throws JsonProcessingException {

        Play play = new Play();
        play.setTimestamp(new Date(System.currentTimeMillis()));
        play.setLastUpdatedTimestamp(new Date(System.currentTimeMillis()));
        play.setDisplayName(DISPLAY_NAME);
        play.setDescription(DESCRIPTION);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);

        LaunchHistory launchHistory = new LaunchHistory();
        launchHistory.setNewAccountsNum(300L);
        launchHistory.setNewContactsNum(400L);
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setDescription("Play Launch Name");
        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
        launchHistory.setPlayLaunch(playLaunch);
        play.setLaunchHistory(launchHistory);

        RatingObject rating = new RatingObject();
        List<BucketInformation> accountRatingList = new ArrayList<>();
        BucketInformation aBucket = new BucketInformation();
        aBucket.setBucket(BucketName.A.name());
        aBucket.setBucketCount(500);
        BucketInformation bBucket = new BucketInformation();
        bBucket.setBucket(BucketName.B.name());
        bBucket.setBucketCount(500);
        BucketInformation cBucket = new BucketInformation();
        cBucket.setBucket(BucketName.C.name());
        cBucket.setBucketCount(500);
        BucketInformation dBucket = new BucketInformation();
        dBucket.setBucket(BucketName.D.name());
        dBucket.setBucketCount(500);
        accountRatingList.add(aBucket);
        accountRatingList.add(bBucket);
        accountRatingList.add(cBucket);
        accountRatingList.add(dBucket);
        rating.setBucketInfoList(accountRatingList);
        play.setRating(rating);

        List<TalkingPoint> talkingPoints = new ArrayList<>();
        TalkingPoint tp = new TalkingPoint();
        tp.setTitle(TALKING_POINT_TITLE);
        tp.setContent(TALKING_POINT_CONTENT);
        talkingPoints.add(tp);
        play.setTalkingPoints(talkingPoints);

        log.info("playOverview is " + play);
    }

}
