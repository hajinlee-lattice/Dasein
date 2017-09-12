package com.latticeengines.query.evaluator;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    private static final String accountId = "12072224";

    @Test(groups = "functional")
    public void testEntityLookup() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        Query query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1916);
    }

    @Test(groups = "functional", enabled = false)
    public void testSelect() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_WEBSITE) //
                .select(BusinessEntity.Account, "LDC_Name", "LDC_City", "LDC_State") //
                .where(restriction).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 1);
        for (Map<String, Object> row : results) {
            Assert.assertEquals(row.size(), 5);
            Assert.assertTrue(row.containsKey(ATTR_ACCOUNT_NAME));
            Assert.assertTrue(row.containsKey(ATTR_ACCOUNT_WEBSITE));
            Assert.assertTrue(row.containsKey("LDC_Name"));
            Assert.assertTrue(row.containsKey("LDC_City"));
            Assert.assertTrue(row.containsKey("LDC_State"));

            Assert.assertEquals(row.get("LDC_Name").toString(), "Universal One Publishing");
            Assert.assertEquals(row.get("LDC_State").toString().toUpperCase(), "MARYLAND");
        }
    }

    @Test(groups = "functional")
    public void testAccountWithSelectedContact() {
        String alias = BusinessEntity.Account.name().concat(String.valueOf(new Date().getTime()));
        Query query = generateAccountWithSelectedContactQuery(alias);
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 1);

    }

    @Test(groups = "functional")
    public void testExistsRestriction() {
        Restriction inner1 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).eq("Director of IT")
                .build();
        Restriction inner2 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_COUNTRY).eq("United States")
                .build();
        Restriction inner = Restriction.builder().and(inner1, inner2).build();
        Restriction restriction = Restriction.builder() //
                .exists(BusinessEntity.Contact) //
                .that(inner) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_ID) //
                .from(BusinessEntity.Account) //
                .where(restriction) //
                .page(new PageFilter(1, 2)) //
                .build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 2);
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("a", "z") //
                .build();
        Query query1 = Query.builder().where(range1).build();
        long count1 = queryEvaluatorService.getCount(attrRepo, query1);
        Assert.assertEquals(count1, 2331);

        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").in(1.0, 9.5) //
                .build();
        Query query2 = Query.builder().where(range2).build();
        long count2 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count2, 306);

        query2 = Query.builder().where(range2).from(BusinessEntity.Account).build();
        count2 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count2, 306);

        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertTrue(count <= count1 && count <= count2);
        Assert.assertEquals(count, 1);
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
        return new Object[][] { { "Yes", BUCKETED_YES_IN_CUSTOEMR }, { "No", BUCKETED_NO_IN_CUSTOEMR },
                { null, BUCKETED_NULL_IN_CUSTOEMR } };
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction domainInRange = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_CITY).in("c", "m") //
                .build();
        Query query1 = Query.builder()
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(domainInRange) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        long countInRedshift = queryEvaluatorService.getCount(attrRepo, query1);
        Assert.assertEquals(countInRedshift, 234);

        List<Map<String, Object>> results;
        int offset = 0;
        int pageSize = 50;
        int totalRuns = 0;
        int totalResults = 0;
        String prevName = null;
        do {
            PageFilter pageFilter = new PageFilter(offset, pageSize);
            Query query = Query.builder()
                    .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                    .where(domainInRange) //
                    .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                    .page(pageFilter) //
                    .build();
            results = queryEvaluatorService.getData(attrRepo, query).getData();
            for (Map<String, Object> result : results) {
                String name = (String) result.get(ATTR_ACCOUNT_NAME);
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
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("a", "e") //
                .build();

        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .build();

        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 641);
        long count = results.stream().filter(result -> {
            String city = (String) result.get(ATTR_ACCOUNT_CITY);
            return city != null && city.toUpperCase().contains("HAM");
        }).count();
        Assert.assertEquals(count, 34);

        query = Query.builder().select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .freeText("ham", BusinessEntity.Account, ATTR_ACCOUNT_CITY) //
                .build();

        results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), count);
    }

    @Test(groups = "functional", enabled = false)
    public void testCaseLookup() {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        Restriction A = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("c", "d").build();
        Restriction B = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE).in("a", "b").build();
        Restriction C = Restriction.builder().let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq("No").build();

        cases.put("B", B);
        cases.put("A", A);
        cases.put("C", C);
        CaseLookup caseLookup = new CaseLookup(cases, "B", "Score");

        Query query = Query.builder() //
                .select(caseLookup) //
                .build();

        // sub query
        SubQuery subQuery = new SubQuery(query, "Alias");
        SubQueryAttrLookup attrLookup = new SubQueryAttrLookup(subQuery, "Score");
        Query query2 = Query.builder() //
                .select(attrLookup, AggregateLookup.count().as("Count")) //
                .from(subQuery) //
                .groupBy(attrLookup) //
                .having(Restriction.builder().let(attrLookup).neq("C").build()) //
                .build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query2).getData();
        Assert.assertEquals(results.size(), 2);
        results.forEach(map -> {
            if ("A".equals(map.get("score"))) {
                Assert.assertEquals(map.get("count"), 1816L);
            } else if ("B".equals(map.get("score"))) {
                Assert.assertEquals(map.get("count"), 62724L);
            }
        });

        // direct group by
        query = Query.builder() //
                .select(caseLookup, AggregateLookup.count().as("Count")) //
                .where(Restriction.builder().or(A, B).build()) //
                .groupBy(caseLookup) //
                .build();
        results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 2);
        results.forEach(map -> {
            if ("A".equals(map.get("score"))) {
                Assert.assertEquals(map.get("count"), 1816L);
            } else if ("B".equals(map.get("score"))) {
                Assert.assertEquals(map.get("count"), 8657L);
            }
        });
    }

}
