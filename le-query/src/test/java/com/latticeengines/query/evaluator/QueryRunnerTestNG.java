package com.latticeengines.query.evaluator;

import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.latticeengines.query.util.QueryUtils;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    @BeforeTest(groups = "functional")
    public void setup() {
        QueryUtils.testmode = true;
    }

    @AfterTest(groups = "functional")
    public void teardown() {
        QueryUtils.testmode = false;
    }

    @Test(groups = "functional")
    public void testEntityLookup() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "ID").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            long count = queryEvaluator.evaluate(attrRepo, query).fetchCount();
            Assert.assertEquals(count, 5);
        }
    }

    @Test(groups = "functional")
    public void testJoinSelect() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "ID").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Contact, "CompanyName", "LastName") //
                .select(BusinessEntity.Account, "City", "State") //
                .where(restriction).build();
        List<Map<String, Object>> results;
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            results = queryEvaluator.run(attrRepo, query).getData(); // count = 5, rowsize = 2 //
        }
        Assert.assertEquals(results.size(), 25);
        for (Map<String, Object> row : results) {
            Assert.assertEquals(row.size(), 4);
            Assert.assertTrue(row.containsKey("CompanyName"));
            Assert.assertTrue(row.containsKey("LastName"));
            Assert.assertTrue(row.containsKey("City"));
            Assert.assertTrue(row.containsKey("State"));
        }
    }

    @Test(groups = "functional")
    public void testJoinLatticeAccountSelect() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "ID").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "CompanyName", "LastName") //
                .select(BusinessEntity.LatticeAccount, "City", "State") //
                .where(restriction).build();
        List<Map<String, Object>> results;
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            results = queryEvaluator.run(attrRepo, query).getData(); // count = 5, row size = 2 //
        }
        Assert.assertEquals(results.size(), 25);
        for (Map<String, Object> row : results) {
            Assert.assertEquals(row.size(), 4);
            Assert.assertTrue(row.containsKey("CompanyName"));
            Assert.assertTrue(row.containsKey("LastName"));
            Assert.assertTrue(row.containsKey("City"));
            Assert.assertTrue(row.containsKey("State"));
        }
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").in("a", "z") //
                .build();
        Query query1 = Query.builder().where(range1).build();
        long count1 = Long.MAX_VALUE;
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            count1 = queryEvaluator.evaluate(attrRepo, query1).fetchCount();
            Assert.assertEquals(count1, 77058);
        }

        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").in(1.0, 3.5) //
                .build();
        Query query2 = Query.builder().where(range2).build();
        long count2 = Long.MAX_VALUE;
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            count2 = queryEvaluator.evaluate(attrRepo, query2).fetchCount();
            Assert.assertEquals(count2, 169976);
        }

        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            long count = queryEvaluator.evaluate(attrRepo, query).fetchCount();
            Assert.assertEquals(count, 68827);
            Assert.assertTrue(count <= count1 && count <= count2);
        }
    }

    @Test(groups = "functional")
    public void testExists() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").in("a", "z") //
                .build();
        Restriction restriction = Restriction.builder() //
                .exists(BusinessEntity.Contact).that(range1) //
                .build();
        Query query = Query.builder().where(restriction).build();
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            long count = queryEvaluator.evaluate(attrRepo, query).fetchCount();
            Assert.assertEquals(count, 77058);
        }
    }

    @Test(groups = "functional", enabled = false)
    public void testBitEncoded() {
        // bucket
        Restriction lbl2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "Bucketed_Attribute").eq("Label2") //
                .build();
        Restriction nullLbl = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "Bucketed_Attribute").eq(null) //
                .build();
        Restriction restriction = Restriction.builder().or(lbl2, nullLbl).build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            @SuppressWarnings("unused")
            List<Map<String, Object>> results = queryEvaluator.run(attrRepo, query).getData();
        }
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction nameIsCity = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").eq(BusinessEntity.Account, "City") //
                .build();
        Query query = Query.builder().select(BusinessEntity.Account, "ID", "CompanyName", "City") //
                .where(nameIsCity) //
                .orderBy(BusinessEntity.Account, "CompanyName") //
                .build();

        List<Map<String, Object>> results;
        int offset = 0;
        int pageSize = 50;
        int totalRuns = 0;
        int totalResults = 0;
        String prevName = null;
        do {
            PageFilter pageFilter = new PageFilter(offset, pageSize);
            query.setPageFilter(pageFilter);
            try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
                results = queryEvaluator.run(attrRepo, query).getData();
            }
            for (Map<String, Object> result : results) {
                String name = result.get("CompanyName").toString();
                if (prevName != null) {
                    Assert.assertTrue(prevName.compareTo(name) <= 0);
                }
                prevName = name;
            }

            totalRuns++;
            totalResults += results.size();
            Assert.assertTrue(results.size() <= pageSize);
            offset += pageSize;
        } while (results.size() > 0);
        Assert.assertEquals(totalResults, 211);
        Assert.assertEquals(totalRuns, (int) (Math.ceil(211.0 / pageSize) + 1));
    }

    @Test(groups = "functional")
    public void testFreeTextSearch() {
        Restriction nameIsCity = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").eq(BusinessEntity.Account, "City") //
                .build();

        Query query = Query.builder().select(BusinessEntity.Account, "ID", "CompanyName", "City") //
                .where(nameIsCity) //
                .build();

        List<Map<String, Object>> results;
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            results = queryEvaluator.run(attrRepo, query).getData();
        }
        Assert.assertEquals(results.size(), 211);
        long count = results.stream().map(m -> m.get("City")).filter(v -> ((String) v).contains("AMBUR")).count();
        Assert.assertEquals(count, 4);

        query = Query.builder().select(BusinessEntity.Account, "ID", "CompanyName", "City") //
                .where(nameIsCity) //
                .freeText("AMBUR") //
                .freeTextAttributes(BusinessEntity.Account, "City") //
                .build();

        results = queryEvaluator.run(attrRepo, query).getData();
        Assert.assertEquals(results.size(), count);
    }
}
