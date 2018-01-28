package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RuleBucketNameUnitTestNG {

    @Test(groups = "unit")
    public void testGetRuleBucketName() {
        Assert.assertEquals(RatingBucketName.valueOf("A"), RatingBucketName.A);
        Assert.assertEquals(RatingBucketName.valueOf("B"), RatingBucketName.B);
        Assert.assertEquals(RatingBucketName.valueOf("C"), RatingBucketName.C);
        Assert.assertEquals(RatingBucketName.valueOf("D"), RatingBucketName.D);
        Assert.assertEquals(RatingBucketName.valueOf("F"), RatingBucketName.F);
        Assert.assertEquals(RatingBucketName.valueOf("G"), RatingBucketName.G);
    }
}
