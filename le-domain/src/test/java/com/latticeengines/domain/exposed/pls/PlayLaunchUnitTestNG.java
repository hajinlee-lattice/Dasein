package com.latticeengines.domain.exposed.pls;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class PlayLaunchUnitTestNG {

    private static final String PLAY_NAME = "play";
    private Play play;
    private PlayLaunch playLaunch;
    private String createdBy = "build@lattice-engines.com";

    @Test(groups = "unit")
    public void test() {
        play = new Play();
        play.setDisplayName(PLAY_NAME);
        play.setName(play.generateNameStr());
        play.setCreatedBy(createdBy);
        play.setUpdatedBy(createdBy);
        playLaunch = new PlayLaunch();
        playLaunch.setPlay(play);
        Set<RatingBucketName> selectedBuckets = new HashSet<>();
        selectedBuckets.add(RatingBucketName.A);
        selectedBuckets.add(RatingBucketName.B);
        selectedBuckets.add(RatingBucketName.C);
        selectedBuckets.add(RatingBucketName.D);
        playLaunch.setBucketsToLaunch(selectedBuckets);
        playLaunch.setCreatedBy(createdBy);
        playLaunch.setUpdatedBy(createdBy);
        playLaunch.setLaunchState(LaunchState.Canceled);

        RecordsStats stats = new RecordsStats();
        stats.setRecordsReceivedFromAcxiom(1000L);
        stats.setAcxiomRecordsSucceedToDestination(900L);
        playLaunch.setRecordsStats(stats);

        String playLaunchStr = playLaunch.toString();
        System.out.println(String.format("playLaunch is %s", playLaunchStr));

        PlayLaunch deserializedPlayLaunch = JsonUtils.deserialize(playLaunchStr, PlayLaunch.class);

        Assert.assertNotNull(deserializedPlayLaunch.getBucketsToLaunch());
        Assert.assertEquals(deserializedPlayLaunch.getBucketsToLaunch().size(), 4);

        Assert.assertNotNull(deserializedPlayLaunch.getRecordsStats());
        Assert.assertEquals(deserializedPlayLaunch.getRecordsStats().getRecordsReceivedFromAcxiom().longValue(), 1000L);
        Assert.assertEquals(deserializedPlayLaunch.getRecordsStats().getAcxiomRecordsSucceedToDestination().longValue(),
                900L);
    }

    @Test(groups = "unit")
    public void testMerge() {
        play = new Play();
        play.setDisplayName(PLAY_NAME);
        play.setName(play.generateNameStr());
        play.setCreatedBy(createdBy);
        play.setUpdatedBy(createdBy);
        playLaunch = new PlayLaunch();
        playLaunch.setPlay(play);
        Set<RatingBucketName> selectedBuckets = new HashSet<>();
        selectedBuckets.add(RatingBucketName.A);
        selectedBuckets.add(RatingBucketName.B);
        selectedBuckets.add(RatingBucketName.C);
        selectedBuckets.add(RatingBucketName.D);
        playLaunch.setBucketsToLaunch(selectedBuckets);
        playLaunch.setCreatedBy(createdBy);
        playLaunch.setUpdatedBy(createdBy);
        playLaunch.setLaunchState(LaunchState.Canceled);

        RecordsStats stats = new RecordsStats();
        stats.setRecordsReceivedFromAcxiom(1000L);
        stats.setAcxiomRecordsSucceedToDestination(900L);
        playLaunch.setRecordsStats(stats);

        PlayLaunch deserializedPlayLaunch = new PlayLaunch();
        deserializedPlayLaunch.merge(playLaunch);

        Assert.assertNotNull(deserializedPlayLaunch.getBucketsToLaunch());
        Assert.assertEquals(deserializedPlayLaunch.getBucketsToLaunch().size(), 4);

        Assert.assertNotNull(deserializedPlayLaunch.getRecordsStats());
        Assert.assertEquals(deserializedPlayLaunch.getRecordsStats().getRecordsReceivedFromAcxiom().longValue(), 1000L);
        Assert.assertEquals(deserializedPlayLaunch.getRecordsStats().getAcxiomRecordsSucceedToDestination().longValue(),
                900L);
    }

}
