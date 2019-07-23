package com.latticeengines.query.evaluator;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

/**
 * This test will go out to a test table in Redshift
 */
public class QueryRunnerTestNG extends QueryFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(QueryRunnerTestNG.class);

    private static final String accountId = "0012400001DNVOPAA5";

    private boolean testIsEnabled = false;

    private String getAccountId() {
        return accountId;
    }

    protected String getBitEncodedNominalAttr() {
        return BUCKETED_NOMINAL_ATTR;
    }

    private long getTotalAccountCount() {
        return TOTAL_RECORDS;
    }

    @DataProvider(name = "userContexts")
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SQL_USER, "Redshift" }
        };
    }

    @BeforeClass(groups = "functional")
    public void setupBase() {
        super.setupBase();
        testIsEnabled = true;
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testStartsWithLookup(String sqlUser, String queryContext) {
        if (!testIsEnabled) {
            log.info("Test is not enabled.");
            return;
        }
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(getAccountId ()) //
                .build();
        Query query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).startsWith(getAccountId().substring(0, getAccountId().length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        long count2 = testGetCountAndAssert(sqlUser, query, 1);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .notcontains(getAccountId().substring(0, getAccountId().length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, getTotalAccountCount() - count2);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testStartsWithLookupContact(String sqlUser, String queryContext) {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).eq(getAccountId()) //
                .build();
        Query query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 13);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).contains(getAccountId().substring(1, getAccountId().length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 13);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testSelect(String sqlUser, String queryContext) {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq(getAccountId()) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE) //
                .select(BusinessEntity.Account, "LDC_Name", "LDC_City", "LDC_State") //
                .where(restriction).build();

        List<Map<String, Object>> expectedResults = new ArrayList<Map<String, Object>>();
        Map<String, Object> resMapRow1 = new HashMap<>();
        resMapRow1.put(ATTR_ACCOUNT_WEBSITE, null);
        resMapRow1.put("LDC_Name", "Lake Region Medical, Inc.");
        resMapRow1.put("LDC_City", "Wilmington");
        resMapRow1.put("LDC_State", "MASSACHUSETTS");
        expectedResults.add(resMapRow1);
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetDataAndAssert(sqlUser, query, 1, expectedResults);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testAccountWithSelectedContact(String sqlUser, String queryContext) {
        String alias = BusinessEntity.Account.name().concat(String.valueOf(new Date().getTime()));
        Query query = generateAccountWithSelectedContactQuery(alias);
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetDataAndAssert(sqlUser, query, 1, null);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testExistsRestriction(String sqlUser, String queryContext) {
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
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetDataAndAssert(sqlUser, query, 2, null);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testRangeLookup(String sqlUser, String queryContext) {
        Restriction range1 = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A"),
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).lt("Z")
        ).build();
        Query query1 = Query.builder().where(range1).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query1, sqlUser);
        logQuery(sqlUser, sqlQuery);
        long count1 = testGetCountAndAssert(sqlUser, query1, 1673);

        Restriction range2 = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, "AlexaViewsPerUser").gte(1.0),
                Restriction.builder().let(BusinessEntity.Account, "AlexaViewsPerUser").lt(9.5)
        ).build();
        Query query2 = Query.builder().where(range2).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query2, sqlUser);
        logQuery(sqlUser, sqlQuery);
        long count2 = testGetCountAndAssert(sqlUser, query2, 1152);

        query2 = Query.builder().where(range2).from(BusinessEntity.Account).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query2, sqlUser);
        logQuery(sqlUser, sqlQuery);
        count2 = testGetCountAndAssert(sqlUser, query2, 1152);

        query2 = Query.builder().where(range2).from(BusinessEntity.Contact).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query2, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query2, 2951);

        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        long count = testGetCountAndAssert(sqlUser, query, 1134);
        Assert.assertTrue(count <= count1 && count <= count2);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "bitEncodedData")
    public void testBitEncoded(String sqlUser, ComparisonType operator, String[] vals, long expectedCount) {
        // bucket
        RestrictionBuilder builder = Restriction.builder();
        String value = vals == null ? null : vals[0];
        List<Object> objs;
        switch (operator) {
            case EQUAL:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).eq(value);
                break;
            case NOT_EQUAL:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).neq(value);
                break;
            case STARTS_WITH:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).startsWith(value);
                break;
            case ENDS_WITH:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).endsWith(value);
                break;
            case CONTAINS:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).contains(value);
                break;
            case NOT_CONTAINS:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).notcontains(value);
                break;
            case IS_NULL:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).isNull();
                break;
            case IS_NOT_NULL:
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).isNotNull();
                break;
            case IN_COLLECTION:
                Assert.assertNotNull(vals);
                objs = Arrays.stream(vals).collect(Collectors.toList());
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).inCollection(objs);
                break;
            case NOT_IN_COLLECTION:
                Assert.assertNotNull(vals);
                objs = Arrays.stream(vals).collect(Collectors.toList());
                builder = builder.let(BusinessEntity.Account, getBitEncodedNominalAttr()).notInCollection(objs);
                break;
            default:
                throw new UnsupportedOperationException("Does not support " + operator);
        }
        Restriction restriction = builder.build();
        Query query = Query.builder().where(restriction).build();
        //DP-9687: Using the same query object to generate SQLQuery, is generating invalid SQLQuery.
        //SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        //logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, expectedCount);
    }

    @DataProvider(name = "bitEncodedData", parallel = false)
    private Object[][] provideBitEncodedData() {
        return getBitEncodedTestData();
    }

    protected Object[][] getBitEncodedTestData() {
        return new Object[][] {
                { SQL_USER, ComparisonType.EQUAL, new String[]{ "Yes" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.EQUAL, new String[]{ "No" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.EQUAL, null, BUCKETED_NULL_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.EQUAL, new String[]{ null }, BUCKETED_NULL_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.EQUAL, new String[]{ "bar" }, 0L }, //

                { SQL_USER, ComparisonType.IS_NULL, null, BUCKETED_NULL_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.IS_NOT_NULL, null, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { SQL_USER, ComparisonType.NOT_EQUAL, new String[]{ "Yes" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_EQUAL, new String[]{ "No" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_EQUAL, new String[]{ "bar" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { SQL_USER, ComparisonType.STARTS_WITH, new String[]{ "z" }, 0L }, //
                { SQL_USER, ComparisonType.STARTS_WITH, new String[]{ "y" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.STARTS_WITH, new String[]{ "N" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.ENDS_WITH, new String[]{ "z" }, 0L }, //
                { SQL_USER, ComparisonType.ENDS_WITH, new String[]{ "S" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.ENDS_WITH, new String[]{ "o" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.CONTAINS, new String[]{ "Z" }, 0L }, //
                { SQL_USER, ComparisonType.CONTAINS, new String[]{ "e" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.CONTAINS, new String[]{ "O" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_CONTAINS, new String[]{ "Z" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_CONTAINS, new String[]{ "e" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_CONTAINS, new String[]{ "O" }, BUCKETED_YES_IN_CUSTOEMR }, //

                { SQL_USER, ComparisonType.IN_COLLECTION, new String[]{ "foo" }, 0L }, //
                { SQL_USER, ComparisonType.IN_COLLECTION, new String[]{ "Yes", "yes", "foo" }, BUCKETED_YES_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.IN_COLLECTION, new String[]{ "YES", "no", "bar" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { SQL_USER, ComparisonType.NOT_IN_COLLECTION, new String[]{ "foo" }, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_IN_COLLECTION, new String[]{ "Yes", "yes", "foo" }, BUCKETED_NO_IN_CUSTOEMR }, //
                { SQL_USER, ComparisonType.NOT_IN_COLLECTION, new String[]{ "YES", "no", "bar" }, 0L }, //
        };
    }

    @Test(groups = { "functional" })
    public void testSortAndPage() {
        String sqlUser = SQL_USER;
        Restriction domainInRange = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_CITY).gt("A") //
                .build();
        Query query1 = Query.builder()
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(domainInRange) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query1, sqlUser);
        logQuery(sqlUser, sqlQuery);
        long countInRedshift = testGetCountAndAssert(sqlUser, query1, 1686);

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
            sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
            logQuery(sqlUser, sqlQuery);
            results = testGetDataAndAssert(sqlUser, query, -1, null);
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

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts", enabled = false)
    public void testAggregation(String sqlUser, String queryContext) {
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
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, queryPositive, sqlUser).getData();
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
        results = queryEvaluatorService.getData(attrRepo, queryNullAggregation, sqlUser).getData();
        Assert.assertEquals(results.size(), 1);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testFreeTextSearch(String sqlUser, String queryContext) {
        Restriction nameInRange = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("A") //
                .build();

        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);

        List<Map<String, Object>> results = testGetDataAndAssert(sqlUser, query, 1684, null);
        long count = results.stream().filter(result -> {
            String city = (String) result.get(ATTR_ACCOUNT_CITY);
            return city != null && city.toUpperCase().contains("HAM");
        }).count();
        //Assert.assertEquals(count, 19);

        query = Query.builder().select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .freeText("ham", new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_CITY)) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        results = testGetDataAndAssert(sqlUser, query, count, null);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testCaseLookup(String sqlUser, String queryContext) {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        Restriction A = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("C").build(),
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).lt("D").build()
        ).build();
        Restriction B = Restriction.builder().and(
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE).gte("a").build(),
                Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_WEBSITE).lt("b").build()
        ).build();
        Restriction C = Restriction.builder().let(BusinessEntity.Account, getBitEncodedNominalAttr()).eq("No").build();

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
                .select(attrLookup, AggregateLookup.count().as("count")) //
                .from(subQuery) //
                .groupBy(attrLookup) //
                .orderBy(new Sort(Collections.singletonList(attrLookup), false))
                .having(Restriction.builder().let(attrLookup).neq("C").build()) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query2, sqlUser);
        logQuery(sqlUser, sqlQuery);
        List<Map<String, Object>> expectedResults = new ArrayList<Map<String, Object>>();
        Map<String, Object> resMapRow1 = new HashMap<>();
        resMapRow1.put("Score", "A");
        resMapRow1.put("count", 197L);
        expectedResults.add(resMapRow1);
        Map<String, Object> resMapRow2 = new HashMap<>();
        resMapRow2.put("Score", "B");
        resMapRow2.put("count", 2643L);
        expectedResults.add(resMapRow2);
        testGetDataAndAssert(sqlUser, query2, 2, expectedResults);
    }

}
