package com.latticeengines.domain.exposed.pls;

import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RuleBucketNameUnitTestNG {

    @Test(groups = "unit")
    public void testGetDefaultRuleBucketNames() {
        Set<String> defaultRuleBucketNames = RuleBucketName.getDefaultRuleBucketNames();
        Assert.assertNotNull(defaultRuleBucketNames);
        Assert.assertEquals(defaultRuleBucketNames.size(), 6);
        Assert.assertTrue(defaultRuleBucketNames.contains("A"));
        Assert.assertTrue(defaultRuleBucketNames.contains("A-"));
        Assert.assertTrue(defaultRuleBucketNames.contains("B"));
        Assert.assertTrue(defaultRuleBucketNames.contains("C"));
        Assert.assertTrue(defaultRuleBucketNames.contains("D"));
        Assert.assertTrue(defaultRuleBucketNames.contains("F"));
    }

    @Test(groups = "unit")
    public void testGetRuleBucketName() {
        Assert.assertEquals(RuleBucketName.getRuleBucketName("A"), RuleBucketName.A);
        Assert.assertEquals(RuleBucketName.getRuleBucketName("A-"), RuleBucketName.A_MINUS);
        Assert.assertEquals(RuleBucketName.getRuleBucketName("B"), RuleBucketName.B);
        Assert.assertEquals(RuleBucketName.getRuleBucketName("C"), RuleBucketName.C);
        Assert.assertEquals(RuleBucketName.getRuleBucketName("D"), RuleBucketName.D);
        Assert.assertEquals(RuleBucketName.getRuleBucketName("F"), RuleBucketName.F);
    }
}
