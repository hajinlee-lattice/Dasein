package com.latticeengines.query.evaluator;

import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class QueryEvaluatorTestNG extends QueryFunctionalTestNGBase {

    private static final String ACCOUNT = BusinessEntity.Account.name();
    private static final String CONTACT = BusinessEntity.Contact.name();

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testLookup() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("from %s as %s", accountTableName, ACCOUNT));
        sqlContains(sqlQuery, String.format("select %s.%s", ACCOUNT, ATTR_ACCOUNT_NAME));

        // entity lookup
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, "select ?");
        sqlContains(sqlQuery, String.format("from %s as %s", accountTableName, ACCOUNT));

        // bucketed attribute
        query = Query.builder() //
                .select(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR) //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery,
                String.format("select (%s.%s&?)>>? as %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR, BUCKETED_NOMINAL_ATTR));
    }

    @Test(groups = "functional")
    public void testRestriction() {
        // simple where clause
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AccountId").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME, "City") //
                .where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("where %s.AccountId", ACCOUNT));

        // concrete eq null
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").eq(null) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser is null", ACCOUNT));

        // concrete on double
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").eq(2.5) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser = ?", ACCOUNT));

        // column eqs column
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).eq(BusinessEntity.Account, "City") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s = %s.City", ACCOUNT, ATTR_ACCOUNT_NAME, ACCOUNT));

        // collection look up with 2 elements
        Restriction inCollection = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .in(Arrays.asList('a', 'c')).build();
        query = Query.builder().where(inCollection).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s in (?, ?)", ACCOUNT, ATTR_ACCOUNT_NAME));

        // collection look up with 1 element, corner case
        Restriction inCollection1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .in(Collections.singleton('a')).build();
        query = Query.builder().where(inCollection1).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s = ?", ACCOUNT, ATTR_ACCOUNT_NAME));

        // range look up
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("a", "z") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").in(1.0, 3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s between ? and ?", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser between ? and ?", ACCOUNT));

        // half range look up
        range1 = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("a") //
                .build();
        range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").lt(3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s >= ?", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser < ?", ACCOUNT));
    }

    @Test(groups = "functional")
    public void testExistsRestriction() {
        Restriction inner1 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_TITLE).eq("Director of IT").build();
        Restriction inner2 = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_COUNTRY).eq("United States").build();
        Restriction inner = Restriction.builder().and(inner1, inner2).build();
        Restriction restriction = Restriction.builder() //
                .exists(BusinessEntity.Contact) //
                .that(inner) //
                .build();
        Query query = Query.builder().from(BusinessEntity.Account).where(restriction).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, "where exists");
        sqlContains(sqlQuery, String.format("%s.%s = ?", CONTACT, ATTR_CONTACT_TITLE));
        sqlContains(sqlQuery, String.format("%s.%s = ?", CONTACT, ATTR_CONTACT_COUNTRY));
        sqlContains(sqlQuery, String.format("%s.%s = %s.%s", ACCOUNT, ATTR_ACCOUNT_ID, CONTACT, ATTR_ACCOUNT_ID));
        sqlNotContain(sqlQuery, "left join");
    }

    @Test(groups = "functional", dataProvider = "bitEncodedData")
    public void testBitEncoded(String testCase, Object[] eqArgs, String expectedRhs) {
        logger.info("Testing " + testCase);
        RestrictionBuilder builder = Restriction.builder();
        if (eqArgs.length == 1) {
            if (eqArgs[0] == null) {
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNull();
            } else {
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq(eqArgs[0]);
            }
        } else {
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq((BusinessEntity) eqArgs[0],
                    (String) eqArgs[1]);
        }
        Restriction restriction = builder.build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("(%s.%s&?)>>? = %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR, expectedRhs));
    }

    @DataProvider(name = "bitEncodedData", parallel = true)
    private Object[][] provideBitEncodedData() {
        return new Object[][] { { "bucket = label", new Object[] { "Yes" }, "?" },
                { "bucket is null", new Object[] { null }, "?" } };
    }

    @Test(groups = "functional")
    public void testNullRestriction() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).isNull() //
                .build();
        Query query = Query.builder().where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s is null", ACCOUNT, ATTR_ACCOUNT_NAME));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).isNotNull() //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.%s is not null", ACCOUNT, ATTR_ACCOUNT_NAME));
    }

    @Test(groups = "functional", enabled = false)
    public void testFreeText() {
        // freetext
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .freeText("intel", BusinessEntity.Account, "LDC_Domain", "LDC_Name") //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("upper(%s.LDC_Domain) like ?", ACCOUNT));
        sqlContains(sqlQuery, String.format("upper(%s.LDC_Name) like ?", ACCOUNT));
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistAttribute() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME, "Street1") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistBucket() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq("blah blah") //
                .build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction nameIsCity = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).eq(BusinessEntity.Account, "City") //
                .build();
        Query query = Query.builder().select(BusinessEntity.Account, "AccountId", ATTR_ACCOUNT_NAME, "City") //
                .where(nameIsCity) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("order by %s.%s asc", ACCOUNT, ATTR_ACCOUNT_NAME));
    }

    // require a non-bucketed numeric attribute to test this later
    @Test(groups = "functional", enabled = false)
    public void testSumAggregation() {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME);
        AttributeLookup aggrAttrLookup = new AttributeLookup(BusinessEntity.Account, "AlexaRank");
        Query query = Query.builder() //
                .select(attrLookup, AggregateLookup.sum(aggrAttrLookup)) //
                .from(BusinessEntity.Account) //
                .groupBy(attrLookup) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("select %s.%s", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, "sum(Account.AlexaRank)");
        sqlContains(sqlQuery, String.format("group by %s.%s", ACCOUNT, ATTR_ACCOUNT_NAME));
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
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, ATTR_ACCOUNT_WEBSITE));
        sqlContains(sqlQuery, String.format("when (%s.%s&?)>>? = ? then ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR));
        sqlContains(sqlQuery, "else ? end");
        sqlContains(sqlQuery, "as Score");
        Assert.assertTrue(
                sqlQuery.toString().indexOf(ATTR_ACCOUNT_NAME) < sqlQuery.toString().indexOf(ATTR_ACCOUNT_WEBSITE));
        Assert.assertTrue(sqlQuery.toString().indexOf(ATTR_ACCOUNT_WEBSITE) < sqlQuery.toString()
                .indexOf(BUCKETED_PHYSICAL_ATTR));

        // sub query
        SubQuery subQuery = new SubQuery(query, "Alias");
        SubQueryAttrLookup attrLookup = new SubQueryAttrLookup(subQuery, "Score");
        Query query2 = Query.builder() //
                .select(attrLookup, AggregateLookup.count().as("Count"), AggregateLookup.sum(attrLookup)) //
                .from(subQuery) //
                .groupBy(attrLookup) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query2);
        sqlContains(sqlQuery, "select Alias.Score");
        sqlContains(sqlQuery, "count(?) as Count");
        sqlContains(sqlQuery, "sum(Alias.Score)");
        sqlContains(sqlQuery, "as Alias");
        sqlContains(sqlQuery, "group by Alias.Score");

        // direct group by
        query = Query.builder() //
                .select(caseLookup, AggregateLookup.count().as("Count")) //
                .where(Restriction.builder().or(A, B).build()) //
                .groupBy(caseLookup) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, ATTR_ACCOUNT_WEBSITE));
        sqlContains(sqlQuery, String.format("when (%s.%s&?)>>? = ? then ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR));
        sqlContains(sqlQuery, "else ? end");
        sqlContains(sqlQuery, "as Score");
        sqlContains(sqlQuery, "group by Score");

    }

    private void sqlContains(SQLQuery<?> query, String content) {
        Assert.assertTrue(query.toString().toLowerCase().contains(content.toLowerCase()), //
                String.format("Cannot find pattern [%s] in query: %s", content, query));
    }

    private void sqlNotContain(SQLQuery<?> query, String content) {
        Assert.assertFalse(query.toString().toLowerCase().contains(content.toLowerCase()), //
                String.format("Should not find pattern [%s] in query: %s", content, query));
    }

}
