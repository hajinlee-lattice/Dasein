package com.latticeengines.query.evaluator;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.query.exposed.translator.TransactionRestrictionTranslator;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    private static final String accountId = "12072224";

    @Test(groups = "functional")
    public void testStartsWithLookup() {
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
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).startsWith(accountId.substring(0, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .notcontains(accountId.substring(0, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, TOTAL_RECORDS - 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1916);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).contains(accountId.substring(1, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 1916);
    }

    @Test(groups = "functional", enabled = false)
    public void testTransactionSelect() throws ParseException {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId("1");
        Restriction restriction = new TransactionRestrictionTranslator(txRestriction).convert(BusinessEntity.Account);
        Restriction countryRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_CITY)
                .eq("LEICESTER").build();
        Restriction idRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(1802).build();
        Restriction cityAndTx = Restriction.builder().and(countryRestriction, restriction).build();
        Restriction idOrCityAndTx = Restriction.builder().or(idRestriction, cityAndTx).build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(idOrCityAndTx).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 2);
    }

    @Test(groups = "functional")
    public void testTimeFilter() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Transaction, TRS_TRANSACTION_DATE)//
                .before(Period.Quarter, 0) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Transaction, ATTR_ACCOUNT_ID) //
                .where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 7);
    }

    @Test(groups = "functional", enabled = false)
    public void testTransactionSelectWithTimeFilter() throws ParseException {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId("1");
        txRestriction.setTimeFilter(
                new TimeFilter(ComparisonType.BEFORE, Period.Quarter, Arrays.asList(new Object[] { 1 })));
        txRestriction.setSpentFilter(
                new AggregationFilter(AggregationSelector.SPENT, AggregationType.EACH, ComparisonType.GREATER_THAN, Collections.singletonList(9)));
        Restriction restriction = new TransactionRestrictionTranslator(txRestriction).convert(BusinessEntity.Account);
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 1);
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
    public void testBitEncoded(String label, String start, String contain, long expectedCount) {
        // bucket
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq(label) //
                .build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, expectedCount);

        if (label != null) {
            restriction = Restriction.builder() //
                    .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).startsWith(start) //
                    .build();
            query = Query.builder().where(restriction).build();
            count = queryEvaluatorService.getCount(attrRepo, query);
            Assert.assertEquals(count, expectedCount);

            restriction = Restriction.builder() //
                    .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).contains(contain) //
                    .build();
            query = Query.builder().where(restriction).build();
            count = queryEvaluatorService.getCount(attrRepo, query);
            Assert.assertEquals(count, expectedCount);

            restriction = Restriction.builder() //
                    .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).notcontains(contain) //
                    .build();
            query = Query.builder().where(restriction).build();
            count = queryEvaluatorService.getCount(attrRepo, query);
            Assert.assertEquals(count, TOTAL_RECORDS - expectedCount);
        }
    }

    @DataProvider(name = "bitEncodedData", parallel = true)
    private Object[][] provideBitEncodedData() {
        return new Object[][] { { "Yes", "y", "e", BUCKETED_YES_IN_CUSTOEMR },
                { "No", "N", "o", BUCKETED_NO_IN_CUSTOEMR }, { null, null, null, BUCKETED_NULL_IN_CUSTOEMR } };
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
    public void testAggregation() {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME);
        AttributeLookup aggrAttrLookup = new AttributeLookup(BusinessEntity.Account, "AlexaViewsPerUser");
        AggregateLookup sumLookup = AggregateLookup.sum(aggrAttrLookup);
        AggregateLookup avgLookup = AggregateLookup.avg(aggrAttrLookup);
        Restriction sumRestriction = Restriction.builder().let(sumLookup).gt(0).build();
        Restriction avgRestriction = Restriction.builder().let(avgLookup).gt(0).build();
        Restriction orRestriction = Restriction.builder().or(sumRestriction, avgRestriction).build();
        Restriction acctPosRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(44602)
                .build();
        Query queryPositive = Query.builder() //
                .select(attrLookup, sumLookup, avgLookup) //
                .from(BusinessEntity.Account) //
                .where(acctPosRestriction).groupBy(attrLookup) //
                .having(orRestriction).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, queryPositive).getData();
        Assert.assertEquals(results.size(), 1);
        Restriction sumNullRestriction = Restriction.builder().let(sumLookup).eq(0).build();
        Restriction avgNullRestriction = Restriction.builder().let(avgLookup).eq(0).build();
        Restriction andNullRestriction = Restriction.builder().or(sumNullRestriction, avgNullRestriction).build();
        Restriction acctNullRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(1802)
                .build();
        Query queryNullAggregation = Query.builder() //
                .select(attrLookup, sumLookup, avgLookup) //
                .from(BusinessEntity.Account) //
                .where(acctNullRestriction).groupBy(attrLookup) //
                .having(andNullRestriction).build();
        results = queryEvaluatorService.getData(attrRepo, queryNullAggregation).getData();
        Assert.assertEquals(results.size(), 1);
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
