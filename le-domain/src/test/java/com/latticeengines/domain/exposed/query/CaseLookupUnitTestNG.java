package com.latticeengines.domain.exposed.query;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RuleBucketName;

public class CaseLookupUnitTestNG {

    private CaseLookup caseLookup = new CaseLookup();

    @Test(groups = "unit")
    public void testSortedBucketToRuleMap() {
        Assert.assertEquals(caseLookup.getDefaultBucketName(), RuleBucketName.C.getName());
        assertOrderOfMap(caseLookup.getBucketToRuleMap());
        System.out.println("caseLookup is " + caseLookup);
        assertOrderOfCaseLookupString(caseLookup.toString());
        CaseLookup cl = JsonUtils.deserialize(caseLookup.toString(), CaseLookup.class);
        assertOrderOfMap(cl.getBucketToRuleMap());

    }

    private void assertOrderOfCaseLookupString(String caseLookupStr) {
        int Aindex = caseLookupStr.indexOf("\"" + RuleBucketName.A.getName() + "\"");
        int AminusIndex = caseLookupStr.indexOf("\"" + RuleBucketName.A_MINUS.getName() + "\"");
        int Bindex = caseLookupStr.indexOf("\"" + RuleBucketName.B.getName() + "\"");
        int Cindex = caseLookupStr.indexOf("\"" + RuleBucketName.C.getName() + "\"");
        int Dindex = caseLookupStr.indexOf("\"" + RuleBucketName.D.getName() + "\"");
        int Findex = caseLookupStr.indexOf("\"" + RuleBucketName.F.getName() + "\"");
        Assert.assertTrue(Aindex < AminusIndex //
                && AminusIndex < Bindex //
                && Bindex < Cindex //
                && Cindex < Dindex //
                && Dindex < Findex);
    }

    private void assertOrderOfMap(Map<String, Restriction> map) {
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
