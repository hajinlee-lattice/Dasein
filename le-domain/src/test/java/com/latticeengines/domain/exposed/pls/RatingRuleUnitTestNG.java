package com.latticeengines.domain.exposed.pls;

import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.Restriction;

public class RatingRuleUnitTestNG {

    private RatingRule ratingRule = new RatingRule();

    @Test(groups = "unit")
    public void testSortedBucketToRuleMap() {
        Assert.assertEquals(ratingRule.getDefaultBucketName(), RatingRule.DEFAULT_BUCKET_NAME);
        Assert.assertNotNull(ratingRule.getBucketToRuleMap());
        Assert.assertEquals(ratingRule.getBucketToRuleMap().size(), 0);
        assertOrderOfMap(RatingRule.generateDefaultBuckets());
        System.out.println("ratingRule is " + ratingRule);
        System.out.println("ratingRule.generateDefaultBuckets() is " + RatingRule.generateDefaultBuckets());
    }

    private void assertOrderOfMap(TreeMap<String, Map<String, Restriction>> map) {
        int count = 0;
        for (String key : map.keySet()) {
            assertEachKey(key, count);
            count++;
        }
    }

    private boolean assertEachKey(String key, int count) {
        switch (count) {
        case 0:
            return key.equals(RatingBucketName.A.name());
        case 1:
            return key.equals(RatingBucketName.B.name());
        case 2:
            return key.equals(RatingBucketName.C.name());
        case 3:
            return key.equals(RatingBucketName.D.name());
        case 4:
            return key.equals(RatingBucketName.F.name());
        case 5:
            return key.equals(RatingBucketName.G.name());
        default:
            return false;
        }
    }
}
