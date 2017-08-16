package com.latticeengines.query.evaluator;

import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testEntityLookup() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AccountId").eq("900001501953029") //
                .build();
        Query query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1);
    }

    @Test(groups = "functional")
    public void testSelect() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AccountId").eq("900001501953029") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "DisplayName", "Website") //
                .select(BusinessEntity.Account, "LDC_Name", "LDC_City", "LDC_State") //
                .where(restriction).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 1);
        for (Map<String, Object> row : results) {
            Assert.assertEquals(row.size(), 5);
            Assert.assertTrue(row.containsKey("DisplayName"));
            Assert.assertTrue(row.containsKey("Website"));
            Assert.assertTrue(row.containsKey("LDC_Name"));
            Assert.assertTrue(row.containsKey("LDC_City"));
            Assert.assertTrue(row.containsKey("LDC_State"));

            Assert.assertEquals(row.get("LDC_Name").toString(), "Universal One Publishing");
            Assert.assertEquals(row.get("LDC_State").toString().toUpperCase(), "MARYLAND");
        }
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").in("a", "z") //
                .build();
        Query query1 = Query.builder().where(range1).build();
        long count1 = queryEvaluatorService.getCount(attrRepo, query1);
        Assert.assertEquals(count1, 26382);

        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").in(1.0, 3.5) //
                .build();
        Query query2 = Query.builder().where(range2).build();
        long count2 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count2, 19661);

        query2 = Query.builder().where(range2).from(BusinessEntity.Account).build();
        count2 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count2, 19661);

        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 4906);
        Assert.assertTrue(count <= count1 && count <= count2);
    }

    @Test(groups = "functional", dataProvider = "bitEncodedData")
    public void testBitEncoded(String label, long expectedCount) {
        // bucket
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq(label) //
                .build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, expectedCount);
    }

    @DataProvider(name = "bitEncodedData", parallel = true)
    private Object[][] provideBitEncodedData() {
        return new Object[][] {
            { "Yes", BUCKETED_YES_IN_CUSTOEMR },
            { "No", BUCKETED_NO_IN_CUSTOEMR },
            { null, BUCKETED_NULL_IN_CUSTOEMR }
        };
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction domainInRange = Restriction.builder() //
                .let(BusinessEntity.Account, "LDC_Domain").in("aa", "ac") //
                .build();
        Query query1 = Query.builder().select(BusinessEntity.Account, "AccountId", "DisplayName", "City") //
                .where(domainInRange) //
                .orderBy(BusinessEntity.Account, "DisplayName") //
                .build();
        long countInRedshift = queryEvaluatorService.getCount(attrRepo, query1);
        Assert.assertEquals(countInRedshift, 400);

        List<Map<String, Object>> results;
        int offset = 0;
        int pageSize = 50;
        int totalRuns = 0;
        int totalResults = 0;
        String prevName = null;
        do {
            PageFilter pageFilter = new PageFilter(offset, pageSize);
            Query query = Query.builder().select(BusinessEntity.Account, "AccountId", "DisplayName", "City") //
                    .where(domainInRange) //
                    .orderBy(BusinessEntity.Account, "DisplayName") //
                    .page(pageFilter) //
                    .build();
            results = queryEvaluatorService.getData(attrRepo, query).getData();
            for (Map<String, Object> result : results) {
                String name = (String) result.get("DisplayName");
                if (name != null) {
                    if (prevName != null) {
                        Assert.assertTrue(prevName.compareTo(name) <= 0);
                    }
                    prevName = name;
                }
            }
            totalRuns++;
            totalResults += results.size();
            Assert.assertTrue(results.size() <= pageSize);
            offset += pageSize;
        } while (results.size() > 0);
        Assert.assertEquals(totalResults, countInRedshift);
        Assert.assertEquals(totalRuns, (int) (Math.ceil(new Long(countInRedshift).doubleValue() / pageSize) + 1));
    }

    @Test(groups = "functional")
    public void testFreeTextSearch() {
        Restriction nameInRange = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").in("a", "d") //
                .build();

        Query query = Query.builder() //
                .select(BusinessEntity.Account, "AccountId", "DisplayName", "City") //
                .where(nameInRange) //
                .build();

        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 6110);
        long count = results.stream().filter(result -> {
            String city = (String) result.get("City");
            return city != null && city.toUpperCase().contains("EAST");
        }).count();
        Assert.assertEquals(count, 3);

        query = Query.builder().select(BusinessEntity.Account, "AccountId", "DisplayName", "City") //
                .where(nameInRange) //
                .freeText("east", BusinessEntity.Account, "City") //
                .build();

        results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), count);
    }
}
