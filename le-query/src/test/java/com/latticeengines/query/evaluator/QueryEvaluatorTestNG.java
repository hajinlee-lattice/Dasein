package com.latticeengines.query.evaluator;

import java.util.Arrays;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AggregateLookup;
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

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testLookup() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "DisplayName") //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("from %s as Account", accountTableName));
        sqlContains(sqlQuery, String.format("select %s.DisplayName", ACCOUNT));

        // entity lookup
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, "select ?");
        sqlContains(sqlQuery, String.format("from %s as Account", accountTableName));

        // bucketed attribute
        query = Query.builder() //
                .select(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR) //
                .select(BusinessEntity.Account, "DisplayName").build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("select (%s.%s&?)>>? as %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR,
                BUCKETED_NOMINAL_ATTR));
    }

    @Test(groups = "functional")
    public void testRestriction() {
        // simple where clause
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AccountId").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "DisplayName", "City") //
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
                .let(BusinessEntity.Account, "DisplayName").eq(BusinessEntity.Account, "Display_Name") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName = %s.Display_Name", ACCOUNT, ACCOUNT));

        // collection look up with 2 elements
        Restriction inCollection = Restriction.builder().let(BusinessEntity.Account, "DisplayName")
                .in(Arrays.asList('a', 'c')).build();
        query = Query.builder().where(inCollection).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName in (?, ?)", ACCOUNT));

        // collection look up with 1 element, corner case
        Restriction inCollection1 = Restriction.builder().let(BusinessEntity.Account, "DisplayName")
                .in(Arrays.asList('a')).build();
        query = Query.builder().where(inCollection1).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName = ?", ACCOUNT));

        // range look up
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").in("a", "z") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").in(1.0, 3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName between ? and ?", ACCOUNT));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser between ? and ?", ACCOUNT));

        // half range look up
        range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").gte("a") //
                .build();
        range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").lt(3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName >= ?", ACCOUNT));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser < ?", ACCOUNT));
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
                .let(BusinessEntity.Account, "DisplayName").isNull() //
                .build();
        Query query = Query.builder().where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName is null", ACCOUNT));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").isNotNull() //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName is not null", ACCOUNT));
    }

    @Test(groups = "functional")
    public void testFreeText() {
        // freetext
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "DisplayName") //
                .freeText("intel", BusinessEntity.Account, "LDC_Domain", "LDC_Name") //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("upper(%s.LDC_Domain) like ?", ACCOUNT));
        sqlContains(sqlQuery, String.format("upper(%s.LDC_Name) like ?", ACCOUNT));
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistAttribute() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "CompanyName", "Street1") //
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
                .let(BusinessEntity.Account, "DisplayName").eq(BusinessEntity.Account, "City") //
                .build();
        Query query = Query.builder().select(BusinessEntity.Account, "AccountId", "DisplayName", "City") //
                .where(nameIsCity) //
                .orderBy(BusinessEntity.Account, "DisplayName") //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("order by %s.DisplayName asc", ACCOUNT));
    }

    @Test(groups = "functional")
    public void testCaseLookup() {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        Restriction A = Restriction.builder().let(BusinessEntity.Account, "DisplayName").in("c", "d").build();
        Restriction B = Restriction.builder().let(BusinessEntity.Account, "Website").in("a", "b").build();
        Restriction C = Restriction.builder().let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq("No").build();

        cases.put("B", B);
        cases.put("A", A);
        cases.put("C", C);
        CaseLookup caseLookup = new CaseLookup(cases, "B", "Score");

        Query query = Query.builder() //
                .select(caseLookup) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, "DisplayName"));
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, "Website"));
        sqlContains(sqlQuery, String.format("when (%s.%s&?)>>? = ? then ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR));
        sqlContains(sqlQuery, "else ? end");
        sqlContains(sqlQuery, "as Score");
        Assert.assertTrue(sqlQuery.toString().indexOf("DisplayName") < sqlQuery.toString().indexOf("Website"));
        Assert.assertTrue(sqlQuery.toString().indexOf("Website") < sqlQuery.toString().indexOf(BUCKETED_PHYSICAL_ATTR));

        // sub query
        SubQuery subQuery = new SubQuery(query, "Alias");
        SubQueryAttrLookup attrLookup = new SubQueryAttrLookup(subQuery, "Score");
        Query query2 = Query.builder() //
                .select(attrLookup, AggregateLookup.count().as("Count")) //
                .from(subQuery) //
                .groupBy(attrLookup) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query2);
        sqlContains(sqlQuery, "select Alias.Score");
        sqlContains(sqlQuery, "count(?) as Count");
        sqlContains(sqlQuery, "as Alias");
        sqlContains(sqlQuery, "group by Alias.Score");

        // direct group by
        query = Query.builder() //
                .select(caseLookup, AggregateLookup.count().as("Count")) //
                .where(Restriction.builder().or(A, B).build()) //
                .groupBy(caseLookup) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, "DisplayName"));
        sqlContains(sqlQuery, String.format("when %s.%s between ? and ? then ?", ACCOUNT, "Website"));
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
