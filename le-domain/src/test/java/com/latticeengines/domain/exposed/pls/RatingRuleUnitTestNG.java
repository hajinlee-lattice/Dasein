package com.latticeengines.domain.exposed.pls;

import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.Restriction;

public class RatingRuleUnitTestNG {

    private RatingRule ratingRule = new RatingRule();

    @Test(groups = "unit")
    public void testSortedBucketToRuleMap() {
        Assert.assertEquals(ratingRule.getDefaultBucketName(), RuleBucketName.C.getName());
        assertOrderOfMap(ratingRule.getBucketToRuleMap());
        System.out.println("ratingRule is " + ratingRule);
        assertOrderOfCaseLookupString(ratingRule.toString());
        RatingRule deserialized = JsonUtils.deserialize(ratingRule.toString(), RatingRule.class);
        assertOrderOfMap(deserialized.getBucketToRuleMap());

    }

    private void assertOrderOfCaseLookupString(String ratingRule) {
        int Aindex = ratingRule.indexOf("\"" + RuleBucketName.A.getName() + "\"");
        int AminusIndex = ratingRule.indexOf("\"" + RuleBucketName.A_MINUS.getName() + "\"");
        int Bindex = ratingRule.indexOf("\"" + RuleBucketName.B.getName() + "\"");
        int Cindex = ratingRule.indexOf("\"" + RuleBucketName.C.getName() + "\"");
        int Dindex = ratingRule.indexOf("\"" + RuleBucketName.D.getName() + "\"");
        int Findex = ratingRule.indexOf("\"" + RuleBucketName.F.getName() + "\"");
        Assert.assertTrue(Aindex < AminusIndex //
                && AminusIndex < Bindex //
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
            return key.equals(RuleBucketName.A.getName());
        case 1:
            return key.equals(RuleBucketName.A_MINUS.getName());
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
