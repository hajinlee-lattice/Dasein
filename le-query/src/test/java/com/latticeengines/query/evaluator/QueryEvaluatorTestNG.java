package com.latticeengines.query.evaluator;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class QueryEvaluatorTestNG extends QueryFunctionalTestNGBase {

    private static final String ACCOUNT = BusinessEntity.Account.name();
    private static final String LATTICE_ACCOUNT = BusinessEntity.LatticeAccount.name();

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testLookup() {
        // simple lookup
        // Query query = Query.builder() //
        // .select(BusinessEntity.Account, "CompanyName") //
        // .select(BusinessEntity.Contact, "LastName") //
        // .build();
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
                .select(BusinessEntity.LatticeAccount, BUCKETED_NOMINAL_ATTR) //
                .select(BusinessEntity.Account, "DisplayName").build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("select (%s.%s&?)>>? as %s", LATTICE_ACCOUNT, BUCKETED_PHYSICAL_ATTR,
                BUCKETED_NOMINAL_ATTR));
        sqlContains(sqlQuery, String.format("left join %s as %s", amTableName, LATTICE_ACCOUNT));
    }

    @Test(groups = "functional")
    public void testSelectOnlyLatticeAccountAttribute() {
        Pair<InterfaceName, InterfaceName> joinKey = BusinessEntity.Account.join(BusinessEntity.LatticeAccount)
                .getJoinKeys().get(0);
        String joinPattern = String.format("on %s.%s = %s.%s", ACCOUNT, joinKey.getRight().name(), LATTICE_ACCOUNT,
                joinKey.getRight().name());
        Query query = Query.builder() //
                .select(BusinessEntity.LatticeAccount, "LDC_Name", "LDC_Domain") //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlNotContain(sqlQuery, joinPattern);

        query = Query.builder() //
                .select(BusinessEntity.LatticeAccount, "LDC_Name", "LDC_Domain") //
                .from(BusinessEntity.Account) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, joinPattern);
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
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").eq(null) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser is null", LATTICE_ACCOUNT));

        // concrete on double
        restriction = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").eq(2.5) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser = ?", LATTICE_ACCOUNT));

        // column eqs column
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").eq(BusinessEntity.Account, "Display_Name") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName = %s.Display_Name", ACCOUNT, ACCOUNT));

        // restriction = Restriction.builder() //
        // .let(BusinessEntity.Account,
        // "CompanyName").eq(BusinessEntity.Contact, "City") //
        // .build();
        // query = Query.builder().where(restriction).build();
        // sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        // System.out.println(sqlQuery);

        // range look up
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").in("a", "z") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").in(1.0, 3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName between ? and ?", ACCOUNT));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser between ? and ?", LATTICE_ACCOUNT));

        // half range look up
        range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "DisplayName").gte("a") //
                .build();
        range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").lt(3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("%s.DisplayName >= ?", ACCOUNT));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser < ?", LATTICE_ACCOUNT));

        // // exists
        // restriction = Restriction.builder() //
        // .exists(BusinessEntity.Contact) //
        // .that(range1) //
        // .build();
        // query = Query.builder().where(restriction).build();
        // sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        // System.out.println(sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "bitEncodedData")
    public void testBitEncoded(String testCase, Object[] eqArgs, String expectedRhs) {
        logger.info("Testing " + testCase);
        RestrictionBuilder builder = Restriction.builder();
        if (eqArgs.length == 1) {
            if (eqArgs[0] == null) {
                builder = builder.let(BusinessEntity.LatticeAccount, BUCKETED_NOMINAL_ATTR).isNull();
            } else {
                builder = builder.let(BusinessEntity.LatticeAccount, BUCKETED_NOMINAL_ATTR).eq(eqArgs[0]);
            }
        } else {
            builder = builder.let(BusinessEntity.LatticeAccount, BUCKETED_NOMINAL_ATTR).eq((BusinessEntity) eqArgs[0],
                    (String) eqArgs[1]);
        }
        Restriction restriction = builder.build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("(%s.%s&?)>>? = %s", LATTICE_ACCOUNT, BUCKETED_PHYSICAL_ATTR, expectedRhs));
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
                .freeText("intel", BusinessEntity.LatticeAccount, "LDC_Domain", "LDC_Name") //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query);
        sqlContains(sqlQuery, String.format("upper(%s.LDC_Domain) like ?", LATTICE_ACCOUNT));
        sqlContains(sqlQuery, String.format("upper(%s.LDC_Name) like ?", LATTICE_ACCOUNT));
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
                .let(BusinessEntity.LatticeAccount, BUCKETED_NOMINAL_ATTR).eq("blah blah") //
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

    private void sqlContains(SQLQuery<?> query, String content) {
        Assert.assertTrue(query.toString().toLowerCase().contains(content.toLowerCase()), //
                String.format("Cannot find pattern [%s] in query: %s", content, query));
    }

    private void sqlNotContain(SQLQuery<?> query, String content) {
        Assert.assertFalse(query.toString().toLowerCase().contains(content.toLowerCase()), //
                String.format("Should not find pattern [%s] in query: %s", content, query));
    }

}
