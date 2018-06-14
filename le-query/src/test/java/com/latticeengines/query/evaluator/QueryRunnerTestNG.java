package com.latticeengines.query.evaluator;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    private static final String accountId = "0012400001DNVOPAA5";

    @Test(groups = "functional")
    public void testStartsWithLookup() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        Query query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).startsWith(accountId.substring(0, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        long count2 = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count2, 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .notcontains(accountId.substring(0, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, TOTAL_RECORDS - count2);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 13);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).contains(accountId.substring(1, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 13);
    }



    @Test(groups = "functional")
    public void testTimeFilter() {
        // This query actually returns all accounts so it doesn't change over time
        Restriction restriction = Restriction.builder() //
            .let(BusinessEntity.Transaction, ATTR_TRANSACTION_DATE)//
                .prior(PeriodStrategy.Template.Quarter.name(), 1) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Transaction, ATTR_ACCOUNT_ID) //
                .where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 108045);
    }

    @Test(groups = "functional")
    public void testSelect() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE) //
                .select(BusinessEntity.Account, "LDC_Name", "LDC_City", "LDC_State") //
                .where(restriction).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query, SQL_USER).getData();
        Assert.assertEquals(results.size(), 1);
        for (Map<String, Object> row : results) {
            Assert.assertEquals(row.size(), 4);
            Assert.assertTrue(row.containsKey(ATTR_ACCOUNT_WEBSITE));
            Assert.assertTrue(row.containsKey("LDC_Name"));
            Assert.assertTrue(row.containsKey("LDC_City"));
            Assert.assertTrue(row.containsKey("LDC_State"));

            Assert.assertEquals(row.get("LDC_Name").toString(), "Lake Region Medical, Inc.");
            Assert.assertEquals(row.get("LDC_State").toString().toUpperCase(), "MASSACHUSETTS");
        }
    }

    @Test(groups = "functional")
    public void testAccountWithSelectedContact() {
        String alias = BusinessEntity.Account.name().concat(String.valueOf(new Date().getTime()));
        Query query = generateAccountWithSelectedContactQuery(alias);
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query, SQL_USER).getData();
        Assert.assertEquals(results.size(), 1);
    }

    @Test(groups = "functional")
    public void testExistsRestriction() {
        Restriction inner = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).eq("Assistant Professor").build();
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
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query, SQL_USER).getData();
        Assert.assertEquals(results.size(), 2);
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        Restriction range1 = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A"),
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).lt("Z")
        ).build();
        Query query1 = Query.builder().where(range1).build();
        long count1 = queryEvaluatorService.getCount(attrRepo, query1, SQL_USER);
        Assert.assertEquals(count1, 1673);

        Restriction range2 = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, "AlexaViewsPerUser").gte(1.0),
                Restriction.builder().let(BusinessEntity.Account, "AlexaViewsPerUser").lt(9.5)
        ).build();
        Query query2 = Query.builder().where(range2).build();
        long count2 = queryEvaluatorService.getCount(attrRepo, query2, SQL_USER);
        Assert.assertEquals(count2, 1152);

        query2 = Query.builder().where(range2).from(BusinessEntity.Account).build();
        count2 = queryEvaluatorService.getCount(attrRepo, query2, SQL_USER);
        Assert.assertEquals(count2, 1152);

        query2 = Query.builder().where(range2).from(BusinessEntity.Contact).build();
        long count3 = queryEvaluatorService.getCount(attrRepo, query2, SQL_USER);
        Assert.assertEquals(count3, 2951);

        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertTrue(count <= count1 && count <= count2);
        Assert.assertEquals(count, 1134);
    }

    @Test(groups = "functional", dataProvider = "bitEncodedData")
    public void testBitEncoded(ComparisonType operator, String[] vals, long expectedCount) {
        // bucket
        RestrictionBuilder builder = Restriction.builder();
        String value = vals == null ? null : vals[0];
        switch (operator) {
            case EQUAL:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq(value);
                break;
            case NOT_EQUAL:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).neq(value);
                break;
            case STARTS_WITH:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).startsWith(value);
                break;
            case ENDS_WITH:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).endsWith(value);
                break;
            case CONTAINS:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).contains(value);
                break;
            case NOT_CONTAINS:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).notcontains(value);
                break;
            case IS_NULL:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNull();
                break;
            case IS_NOT_NULL:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNotNull();
                break;
            case IN_COLLECTION:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).inCollection(Arrays.asList(vals));
                break;
            case NOT_IN_COLLECTION:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).notInCollection(Arrays.asList(vals));
                break;
            default:
                throw new UnsupportedOperationException("Does not support " + operator);
        }
        Restriction restriction = builder.build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, expectedCount);
    }

    @DataProvider(name = "bitEncodedData", parallel = true)
    private Object[][] provideBitEncodedData() {
        return new Object[][] {
                { ComparisonType.EQUAL, new String[]{ "Yes" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.EQUAL, new String[]{ "No" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.EQUAL, null, BUCKETED_NULL_IN_CUSTOEMR }, //
                { ComparisonType.EQUAL, new String[]{ null }, BUCKETED_NULL_IN_CUSTOEMR }, //
                { ComparisonType.EQUAL, new String[]{ "bar" }, 0L }, //

                { ComparisonType.IS_NULL, null, BUCKETED_NULL_IN_CUSTOEMR }, //
                { ComparisonType.IS_NOT_NULL, null, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { ComparisonType.NOT_EQUAL, new String[]{ "Yes" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_EQUAL, new String[]{ "No" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.NOT_EQUAL, new String[]{ "bar" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { ComparisonType.STARTS_WITH, new String[]{ "z" }, 0L }, //
                { ComparisonType.STARTS_WITH, new String[]{ "y" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.STARTS_WITH, new String[]{ "N" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.ENDS_WITH, new String[]{ "z" }, 0L }, //
                { ComparisonType.ENDS_WITH, new String[]{ "S" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.ENDS_WITH, new String[]{ "o" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.CONTAINS, new String[]{ "Z" }, 0L }, //
                { ComparisonType.CONTAINS, new String[]{ "e" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.CONTAINS, new String[]{ "O" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_CONTAINS, new String[]{ "Z" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_CONTAINS, new String[]{ "e" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_CONTAINS, new String[]{ "O" }, BUCKETED_YES_IN_CUSTOEMR }, //

                { ComparisonType.IN_COLLECTION, new String[]{ "foo" }, 0L }, //
                { ComparisonType.IN_COLLECTION, new String[]{ "Yes", "yes", "foo" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.IN_COLLECTION, new String[]{ "YES", "no", "bar" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { ComparisonType.NOT_IN_COLLECTION, new String[]{ "foo" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_IN_COLLECTION, new String[]{ "Yes", "yes", "foo" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_IN_COLLECTION, new String[]{ "YES", "no", "bar" }, 0L }, //
        };
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction domainInRange = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_CITY).gt("A") //
                .build();
        Query query1 = Query.builder()
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(domainInRange) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        long countInRedshift = queryEvaluatorService.getCount(attrRepo, query1, SQL_USER);
        Assert.assertEquals(countInRedshift, 1686);

        List<Map<String, Object>> results;
        int offset = 0;
        int pageSize = 500;
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
            results = queryEvaluatorService.getData(attrRepo, query, SQL_USER).getData();
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

    @Test(groups = "functional", enabled = false)
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
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, queryPositive, SQL_USER).getData();
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
        results = queryEvaluatorService.getData(attrRepo, queryNullAggregation, SQL_USER).getData();
        Assert.assertEquals(results.size(), 1);
    }

    @Test(groups = "functional")
    public void testFreeTextSearch() {
        Restriction nameInRange = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A") //
                .build();

        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .build();

        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query, SQL_USER).getData();
        Assert.assertEquals(results.size(), 1684);
        long count = results.stream().filter(result -> {
            String city = (String) result.get(ATTR_ACCOUNT_CITY);
            return city != null && city.toUpperCase().contains("HAM");
        }).count();
        Assert.assertEquals(count, 19);

        query = Query.builder().select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .freeText("ham", BusinessEntity.Account, ATTR_ACCOUNT_CITY) //
                .build();

        results = queryEvaluatorService.getData(attrRepo, query, SQL_USER).getData();
        Assert.assertEquals(results.size(), count);
    }

    @Test(groups = "functional")
    public void testCaseLookup() {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        Restriction A = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("C").build(),
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).lt("D").build()
        ).build();
        Restriction B = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE).gte("a").build(),
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE).lt("b").build()
        ).build();
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
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query2, SQL_USER).getData();
        Assert.assertEquals(results.size(), 2);
        results.forEach(map -> {
            if ("A".equals(map.get("score"))) {
                Assert.assertEquals(map.get("count"), 1816L);
            } else if ("B".equals(map.get("score"))) {
                Assert.assertEquals(map.get("count"), 62724L);
            }
        });
    }

}
