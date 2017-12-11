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

    @SuppressWarnings("unused")
    private void assertOrderOfRatingRuleString(String ratingRule) {
        int AplusIndex = ratingRule.indexOf("\"" + RuleBucketName.A_PLUS.getName() + "\"");        
        int Aindex = ratingRule.indexOf("\"" + RuleBucketName.A.getName() + "\"");
        int Bindex = ratingRule.indexOf("\"" + RuleBucketName.B.getName() + "\"");
        int Cindex = ratingRule.indexOf("\"" + RuleBucketName.C.getName() + "\"");
        int Dindex = ratingRule.indexOf("\"" + RuleBucketName.D.getName() + "\"");
        int Findex = ratingRule.indexOf("\"" + RuleBucketName.F.getName() + "\"");
        Assert.assertTrue(AplusIndex < Aindex //
                && Aindex < Bindex //
                && Bindex < Cindex //
                && Cindex < Dindex //
                && Dindex < Findex);
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
            return key.equals(RuleBucketName.A_PLUS.getName());
        case 1:
            return key.equals(RuleBucketName.A.getName());
        case 2:
            return key.equals(RuleBucketName.B.getName());
        case 3:
            return key.equals(RuleBucketName.C.getName());
        case 4:
            return key.equals(RuleBucketName.D.getName());
        case 5:
            return key.equals(RuleBucketName.F.getName());
        default:
            return false;
        }
    }
}
