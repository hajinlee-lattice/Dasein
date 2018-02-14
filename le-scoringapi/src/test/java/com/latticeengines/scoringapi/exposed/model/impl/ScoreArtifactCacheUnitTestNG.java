package com.latticeengines.scoringapi.exposed.model.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ScoreArtifactCacheUnitTestNG {

    private ScoreArtifactCache scoreArtifactCache = new ScoreArtifactCache();

    @BeforeClass(groups = "unit")
    public void setup() {
    }

    @Test(groups = "unit")
    public void testThrottleLargePmmlFileBasedOnWeight() {
        scoreArtifactCache.setScoreArtifactCacheMaxWeight(4);
        scoreArtifactCache.setScoreArtifactCacheMaxCacheThreshold(0.5);
        // TODO Going to reenale the check after making sure that the behavior
        // of Guava cache is clear
        // try {
        // scoreArtifactCache.throttleLargePmmlFileBasedOnWeight(2, "modelId");
        // Assert.fail("Should have thrown exception");
        // } catch (Exception e) {
        // Assert.assertTrue(e instanceof LedpException);
        // Assert.assertEquals(((LedpException) e).getCode(),
        // LedpCode.LEDP_31026);
        // }

        long weight = scoreArtifactCache.throttleLargePmmlFileBasedOnWeight(1, "modelId");
        Assert.assertEquals(weight, 1);

        scoreArtifactCache.setScoreArtifactCacheMaxWeight(4_000_000_000l);
        scoreArtifactCache.setScoreArtifactCacheMaxCacheThreshold(0.6);
        long testWeight = (long) Math.pow(2, 31);
        weight = scoreArtifactCache.throttleLargePmmlFileBasedOnWeight(testWeight, "modelId");
        Assert.assertEquals(weight, Integer.MAX_VALUE);
    }
}
