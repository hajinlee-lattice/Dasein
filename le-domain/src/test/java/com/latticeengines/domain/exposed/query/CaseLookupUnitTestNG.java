package com.latticeengines.domain.exposed.query;

import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CaseLookupUnitTestNG {

    @Test(groups = "unit")
    public void testSortedBucketToRuleMap() {
        TreeMap<String, Restriction> caseMap = new TreeMap<>();
        caseMap.put("A", Restriction.builder().let(BusinessEntity.Account, "A").eq("1").build());
        caseMap.put("C", Restriction.builder().let(BusinessEntity.Contact, "B").eq("2").build());
        CaseLookup caseLookup = new CaseLookup(caseMap, "C", "Rating");
        Query query = Query.builder().select(BusinessEntity.Account, "C").select(caseLookup).build();
        query.analyze();

        Assert.assertTrue(query.getEntitiesForJoin().contains(BusinessEntity.Contact));
    }
}
