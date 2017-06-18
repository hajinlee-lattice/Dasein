package com.latticeengines.query.evaluator;

import static com.latticeengines.query.evaluator.QueryTestUtils.getAttributeRepo;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.Map;

import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.latticeengines.query.util.QueryUtils;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    private AttributeRepository attrRepo;

    @BeforeTest(groups = "functional")
    public void setup() {
        attrRepo = getAttributeRepo();
        QueryUtils.testmode = true;
    }

    @AfterTest(groups = "functional")
    public void teardown() {
        QueryUtils.testmode = false;
    }

    @Test(groups = "functional")
    public void testEntityLookup() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "id").eq("59129793") //
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
                .let(BusinessEntity.Account, "id").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Contact, "companyname", "city") //
                .where(restriction).build();
        List<Map<String, Object>> results;
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            results = queryEvaluator.run(attrRepo, query).getData(); // count = 5, row size = 2
        }
        Assert.assertEquals(results.size(), 25);
        for (Map<String, Object> row: results) {
            Assert.assertEquals(row.size(), 2);
            Assert.assertTrue(row.containsKey("companyname"));
            Assert.assertTrue(row.containsKey("city"));
        }
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").in("a", "z") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "alexaviewsperuser").in(1.0, 3.5) //
                .build();
        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            long count = queryEvaluator.evaluate(attrRepo, query).fetchCount();
            Assert.assertEquals(count, 68827);
        }
    }

    @Test(groups = "functional")
    public void testExists() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").in("a", "z") //
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
                .let(BusinessEntity.LatticeAccount, "bucketed_attribute").eq("Label2") //
                .build();
        Restriction nullLbl = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "bucketed_attribute").eq(null) //
                .build();
        Restriction restriction = Restriction.builder().or(lbl2, nullLbl).build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            List<Map<String, Object>> results = queryEvaluator.run(attrRepo, query).getData();
        }
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction nameIsCity = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").eq(BusinessEntity.Account, "city") //
                .build();
        Query query = Query.builder()
                .select(BusinessEntity.Account, "id", "companyname", "city") //
                .where(nameIsCity) //
                .orderBy(BusinessEntity.Account, "companyname") //
                .build();

        List<Map<String, Object>> results;
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            results = queryEvaluator.run(attrRepo, query).getData();
        }
        assertEquals(results.size(), 211);
        String lastName = null;
        for (Map<String, Object> result : results) {
            String name = result.get("companyname").toString();
            if (lastName != null) {
                assertTrue(lastName.compareTo(name) <= 0);
            }
            lastName = name;
        }
    }

}
