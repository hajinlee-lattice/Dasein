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
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.WindowFunctionLookup;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class QueryEvaluatorTestNG extends QueryFunctionalTestNGBase {

    private static final String ACCOUNT = BusinessEntity.Account.name();
    private static final String CONTACT = BusinessEntity.Contact.name();
    private static final String TRANSACTION = BusinessEntity.Transaction.name();

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testLookup() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .distinct(true) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("from %s as %s", accountTableName, ACCOUNT));
        sqlContains(sqlQuery, String.format("select distinct %s.%s", ACCOUNT, ATTR_ACCOUNT_NAME));

        // entity lookup
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .distinct(true) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, "select distinct ?");
        sqlContains(sqlQuery, String.format("from %s as %s", accountTableName, ACCOUNT));

        // bucketed attribute
        query = Query.builder() //
                .select(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR) //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("select (%s.%s>>?)&? as %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR,
                BUCKETED_NOMINAL_ATTR));
    }

    @Test(groups = "functional")
    public void testLookupWithJoin() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, "ContactId").eq("01B8CAA6F252E5333F722FD8C9DA1707") //
                .build();
        Query query = Query.builder().from(BusinessEntity.Contact)
                .select(BusinessEntity.Contact, ATTR_CONTACT_ID, ATTR_CONTACT_EMAIL) //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .select(BusinessEntity.Transaction, ATTR_TRANSACTION_DATE) //
                .select(BusinessEntity.Transaction, ATTR_PRODUCT_ID) //
                .where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery,
                String.format("select %s.%s, %s.%s", CONTACT, ATTR_CONTACT_ID, CONTACT, ATTR_CONTACT_EMAIL));
        sqlContains(sqlQuery, String.format("from %s as %s", contactTableName, CONTACT));
        sqlContains(sqlQuery, String.format("join %s as %s", accountTableName, ACCOUNT));
        sqlContains(sqlQuery, String.format("on %s.%s = %s.%s", CONTACT, ATTR_ACCOUNT_ID, ACCOUNT, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery, String.format("left join %s as %s", transactionTableName, TRANSACTION));
        sqlContains(sqlQuery,
                String.format("on %s.%s = %s.%s", ACCOUNT, ATTR_ACCOUNT_ID, TRANSACTION, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery, String.format("where lower(%s.%s) = lower(?)", CONTACT, ATTR_CONTACT_ID));
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
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("where lower(%s.AccountId)", ACCOUNT));

        // concrete eq null
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").eq(null) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser is null", ACCOUNT));

        // concrete on double
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").eq(2.5) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser = ?", ACCOUNT));

        // column eqs column
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).eq(BusinessEntity.Account, "City") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("lower(%s.%s) = lower(%s.City)", ACCOUNT, ATTR_ACCOUNT_NAME, ACCOUNT));

        // collection look up with 2 elements
        Restriction inCollection = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .inCollection(Arrays.asList("a", "c")).build();
        query = Query.builder().where(inCollection).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("lower(%s.%s) in (?, ?)", ACCOUNT, ATTR_ACCOUNT_NAME));

        // collection look up with 1 element, corner case
        Restriction inCollection1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .inCollection(Collections.singleton('a')).build();
        query = Query.builder().where(inCollection1).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("lower(%s.%s) = lower(?)", ACCOUNT, ATTR_ACCOUNT_NAME));

        // not in collection lookup
        Restriction notInCollection = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .notInCollection(Arrays.asList('a', 'c')).build();
        query = Query.builder().where(notInCollection).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("lower(%s.%s) not in (?, ?)", ACCOUNT, ATTR_ACCOUNT_NAME));

        Restriction notInCollection1 = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .notInCollection(Collections.singleton('a')).build();
        query = Query.builder().where(notInCollection1).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("not lower(%s.%s) = lower(?)", ACCOUNT, ATTR_ACCOUNT_NAME));

        // half range look up
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("a") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").lt(3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.%s >= ?", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("%s.AlexaViewsPerUser < ?", ACCOUNT));

        // startsWith
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).startsWith("a") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.%s ilike ? || ?", ACCOUNT, ATTR_ACCOUNT_NAME));

        // endsWith
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).endsWith("a") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.%s ilike ? || ?", ACCOUNT, ATTR_ACCOUNT_NAME));
        sqlQuery.getResults();

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).contains("a") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.%s ilike ? || ? || ?", ACCOUNT, ATTR_ACCOUNT_NAME));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).notcontains("a") //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("not %s.%s ilike ? || ?", ACCOUNT, ATTR_ACCOUNT_NAME));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).contains("e") //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("(%s.%s>>?)&? = %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR, "?"));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).notcontains("e") //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("not (%s.%s>>?)&? = %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR, "?"));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).startsWith("y") //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("(%s.%s>>?)&? = %s", ACCOUNT, BUCKETED_PHYSICAL_ATTR, "?"));
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
        Query query = Query.builder().from(BusinessEntity.Account).where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, "where exists");
        sqlContains(sqlQuery, String.format("lower(%s.%s) = lower(?)", CONTACT, ATTR_CONTACT_TITLE));
        sqlContains(sqlQuery, String.format("lower(%s.%s) = lower(?)", CONTACT, ATTR_CONTACT_COUNTRY));
        sqlContains(sqlQuery, String.format("%s.%s = %s.%s", ACCOUNT, ATTR_ACCOUNT_ID, CONTACT, ATTR_ACCOUNT_ID));
        sqlNotContain(sqlQuery, "join");
    }

    @Test(groups = "functional", dataProvider = "bitEncodedData")
    public void testBitEncoded(String testCase, ComparisonType operator, Object[] vals, String expectedPattern) {
        logger.info("Testing " + testCase);
        RestrictionBuilder builder = Restriction.builder();
        switch (operator) {
        case EQUAL:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq(vals[0]);
            break;
        case NOT_EQUAL:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).neq(vals[0]);
            break;
        case IS_NULL:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNull();
            break;
        case IS_NOT_NULL:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNotNull();
            break;
        case STARTS_WITH:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).startsWith(vals[0]);
            break;
        case ENDS_WITH:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).endsWith(vals[0]);
            break;
        case CONTAINS:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).contains(vals[0]);
            break;
        case NOT_CONTAINS:
            builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).notcontains(vals[0]);
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
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, expectedPattern);
    }

    @DataProvider(name = "bitEncodedData", parallel = false)
    private Object[][] provideBitEncodedData() {
        String equalPattern = String.format("(%s.%s>>?)&? = ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR);
        String notNullPattern = String.format("(%s.%s>>?)&? != ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR);
        String notEqualPattern = String.format("%s and %s", notNullPattern, notNullPattern);
        String inCollectionPattern = String.format("(%s.%s>>?)&? in (?, ?)", ACCOUNT, BUCKETED_PHYSICAL_ATTR);
        String notInCollectionPattern = String.format("(%s.%s>>?)&? not in (?, ?)", ACCOUNT, BUCKETED_PHYSICAL_ATTR);
        String notNullAndNotEqualPattern = String.format("not (%s.%s>>?)&? = ? and (%s.%s>>?)&? != ?", //
                ACCOUNT, BUCKETED_PHYSICAL_ATTR, ACCOUNT, BUCKETED_PHYSICAL_ATTR);
        return new Object[][] { //
                { "bucket = label", ComparisonType.EQUAL, new Object[] { "Yes" }, equalPattern }, //
                { "bucket is null", ComparisonType.EQUAL, new Object[] { null }, equalPattern }, //
                { "bucket is null", ComparisonType.IS_NULL, null, equalPattern }, //
                { "bucket is not null", ComparisonType.NOT_EQUAL, new Object[] { null }, notNullPattern }, //
                { "bucket is not null", ComparisonType.IS_NOT_NULL, null, notNullPattern }, //
                { "bucket != label", ComparisonType.NOT_EQUAL, new Object[] { "Yes" }, notEqualPattern }, //
                { "bucket contains non-existing value", ComparisonType.CONTAINS, new Object[] { "Z" }, equalPattern }, //
                { "bucket contains existing value", ComparisonType.CONTAINS, new Object[] { "y" }, equalPattern }, //
                { "bucket not contains non-existing value", ComparisonType.NOT_CONTAINS, new Object[] { "Z" },
                        notNullAndNotEqualPattern }, //
                { "bucket not contains existing value", ComparisonType.NOT_CONTAINS, new Object[] { "y" },
                        notNullAndNotEqualPattern }, //
                { "bucket in collection", ComparisonType.IN_COLLECTION, new Object[] { "YES", "no" },
                        inCollectionPattern }, //
                { "bucket not in collection", ComparisonType.NOT_IN_COLLECTION, new Object[] { "yes", "no" },
                        notInCollectionPattern } //
        };
    }

    @Test(groups = "functional")
    public void testNullRestriction() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).isNull() //
                .build();
        Query query = Query.builder().where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.%s is null", ACCOUNT, ATTR_ACCOUNT_NAME));

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).isNotNull() //
                .build();
        query = Query.builder().where(restriction).build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("%s.%s is not null", ACCOUNT, ATTR_ACCOUNT_NAME));
    }

    @Test(groups = "functional")
    public void testFreeText() {
        // freetext
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .freeText("intel", //
                        new AttributeLookup(BusinessEntity.Account, "LDC_Domain"), //
                        new AttributeLookup(BusinessEntity.Account, "LDC_Name"), //
                        new AttributeLookup(BusinessEntity.Contact, "Title") //
                ) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, "join query_test_contact as Contact");
        sqlContains(sqlQuery, "Account.LDC_Domain ilike ?");
        sqlContains(sqlQuery, "Account.LDC_Name ilike ?");
        sqlContains(sqlQuery, "Contact.Title ilike ?");
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistAttribute() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_NAME, "Street1") //
                .build();
        queryEvaluator.evaluate(attrRepo, query, SQL_USER);
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
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("order by %s.%s asc", ACCOUNT, ATTR_ACCOUNT_NAME));
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
        Restriction acctRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq("44602")
                .build();
        Query query = Query.builder() //
                .select(attrLookup, sumLookup, avgLookup) //
                .from(BusinessEntity.Account) //
                .where(acctRestriction).groupBy(attrLookup) //
                .having(orRestriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("select %s.%s, %s, %s", ACCOUNT, ATTR_ACCOUNT_NAME,
                "coalesce(sum(Account.AlexaViewsPerUser), ?)", "coalesce(avg(Account.AlexaViewsPerUser), ?)"));
        sqlContains(sqlQuery, String.format("from %s as %s", accountTableName, ACCOUNT));
        sqlContains(sqlQuery, String.format("where lower(%s.%s) = lower(?)", ACCOUNT, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery,
                "having coalesce(sum(Account.AlexaViewsPerUser), ?) > ? or coalesce(avg(Account.AlexaViewsPerUser), ?) > ?");
        sqlContains(sqlQuery, String.format("group by %s.%s", ACCOUNT, ATTR_ACCOUNT_NAME));
    }

    @Test(groups = "functional")
    public void testContactExistsJoin() {
        AttributeLookup contactIdAttrLookup = new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_ID);
        AttributeLookup contactEmailAttrLookup = new AttributeLookup(BusinessEntity.Contact, ATTR_CONTACT_EMAIL);
        Restriction accountNameRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME)
                .eq("INTERNATIONAL BANK FOR RECONSTRUCTION & DEVELOPMEN").build();
        Restriction existsRestriction = Restriction.builder().exists(BusinessEntity.Account)
                .that(accountNameRestriction).build();
        Restriction contactRestriction = Restriction.builder().let(BusinessEntity.Contact, ATTR_CONTACT_EMAIL)
                .eq("avelayudham@worldbank.org.kb").build();
        Restriction existsContactWithAccount = Restriction.builder().and(contactRestriction, existsRestriction).build();
        Query query = Query.builder().where(existsContactWithAccount)
                .select(contactIdAttrLookup, contactEmailAttrLookup).from(BusinessEntity.Contact) //
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("select %s.%s", CONTACT, ATTR_CONTACT_ID));

    }

    @Test(groups = "functional")
    public void testAccountWithSelectedContact() {
        String subSelectAlias = "alias";
        Query query = generateAccountWithSelectedContactQuery(subSelectAlias);

        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("select %s.%s", ACCOUNT, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery, String.format("from %s as %s", accountTableName, ACCOUNT));
        sqlContains(sqlQuery, String.format("where lower(%s.%s) = lower(?) ", ACCOUNT, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery, String.format("%s.%s in (", ACCOUNT, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery, String.format("(select %s.%s", subSelectAlias, ATTR_ACCOUNT_ID));
        sqlContains(sqlQuery, String.format("from %s as %s", contactTableName, CONTACT));
        sqlContains(sqlQuery, String.format("where lower(%s.%s) = lower(?))", CONTACT, ATTR_CONTACT_EMAIL));

    }

    @Test(groups = "functional")
    public void testWindowFunction() {
        // TODO: restriction based on Transaction (daily aggregated table) is to
        // be deprecated.
        String accountTotalAmount = "AccountTotalAmount";
        String maxAmount = "MaxAmount";
        String caseAmount = "CaseAmount";
        AttributeLookup amount = new AttributeLookup(BusinessEntity.Transaction, ATTR_TOTAL_AMOUNT);
        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Transaction, ATTR_ACCOUNT_ID);
        TreeMap<String, Restriction> caseMap = new TreeMap<>();
        Restriction amountCaseRestriction = Restriction.builder().let(amount).gt(1000).build();
        caseMap.put("1", amountCaseRestriction);
        CaseLookup caseAmountLookup = new CaseLookup(caseMap, "0", caseAmount);

        WindowFunctionLookup sumLookup = WindowFunctionLookup.sum(amount, accountId, accountTotalAmount);
        WindowFunctionLookup maxLookup = WindowFunctionLookup.max(amount, maxAmount);
        Restriction accountRestriction = Restriction.builder().let(accountId).eq("0012400001DNJYKAA5").build();
        Query query = Query.builder().select(accountId, sumLookup, maxLookup, caseAmountLookup)
                .from(BusinessEntity.Transaction).where(accountRestriction).build();

        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery,
                String.format("select %s.%s as %s, sum(%s.%s) over (partition by %s.%s) as %s,", TRANSACTION,
                        ATTR_ACCOUNT_ID, ATTR_ACCOUNT_ID, TRANSACTION, ATTR_TOTAL_AMOUNT, TRANSACTION, ATTR_ACCOUNT_ID,
                        accountTotalAmount));
        sqlContains(sqlQuery, String.format("max(%s.%s) over () as %s", TRANSACTION, ATTR_TOTAL_AMOUNT, maxAmount));
        sqlContains(sqlQuery, String.format("(case when %s.%s > ? then ? else ? end) as %s", TRANSACTION,
                ATTR_TOTAL_AMOUNT, caseAmount));
        sqlContains(sqlQuery, String.format("from %s as %s", transactionTableName, TRANSACTION));
        sqlContains(sqlQuery, String.format("where lower(%s.%s) = lower(?)", TRANSACTION, ATTR_ACCOUNT_ID));
    }

    @Test(groups = "functional")
    public void testCaseLookup() {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        Restriction A = Restriction.builder()
                .and(Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).gte("c").build(),
                        Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).lte("d").build())
                .build();
        Restriction B = Restriction.builder()
                .and(Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_CITY).gt("a").build(),
                        Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_CITY).lt("b").build())
                .build();
        Restriction C = Restriction.builder().let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).eq("No").build();

        cases.put("B", B);
        cases.put("A", A);
        cases.put("C", C);
        CaseLookup caseLookup = new CaseLookup(cases, "B", "Score");

        Restriction restriction = Restriction.builder().let(caseLookup).eq("A").build();
        Query query = Query.builder() //
                .select(caseLookup) //
                .where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("when (%s.%s >= ? and %s.%s <= ?)", ACCOUNT, ATTR_ACCOUNT_NAME, ACCOUNT,
                ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("when (%s.%s > ? and %s.%s < ?)", ACCOUNT, ATTR_ACCOUNT_CITY, ACCOUNT,
                ATTR_ACCOUNT_CITY));
        sqlContains(sqlQuery, String.format("when (%s.%s>>?)&? = ? then ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR));
        sqlContains(sqlQuery, "else ? end");
        sqlContains(sqlQuery, "as Score");
        sqlContains(sqlQuery, "then ? else ? end) = ?");
        Assert.assertTrue(
                sqlQuery.toString().indexOf(ATTR_ACCOUNT_NAME) < sqlQuery.toString().indexOf(ATTR_ACCOUNT_CITY));
        Assert.assertTrue(
                sqlQuery.toString().indexOf(ATTR_ACCOUNT_CITY) < sqlQuery.toString().indexOf(BUCKETED_PHYSICAL_ATTR));

        // sub query
        SubQuery subQuery = new SubQuery(query, "Alias");
        SubQueryAttrLookup attrLookup = new SubQueryAttrLookup(subQuery, "Score");
        Query query2 = Query.builder() //
                .select(attrLookup, AggregateLookup.count().as("Count"), AggregateLookup.sum(attrLookup)) //
                .from(subQuery) //
                .groupBy(attrLookup) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query2, SQL_USER);
        sqlContains(sqlQuery, "select Alias.Score");
        sqlContains(sqlQuery, "count(?) as Count");
        sqlContains(sqlQuery, "sum(Alias.Score)");
        sqlContains(sqlQuery, "as Alias");
        sqlContains(sqlQuery, "group by Alias.Score");

        // direct group by
        query = Query.builder() //
                .select(caseLookup, AggregateLookup.count().as("Count")) //
                .where(Restriction.builder().or(A, B).build()) //
                .groupBy(new AttributeLookup(null, "Score")) //
                .build();
        sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        sqlContains(sqlQuery, String.format("when (%s.%s >= ? and %s.%s <= ?)", ACCOUNT, ATTR_ACCOUNT_NAME, ACCOUNT,
                ATTR_ACCOUNT_NAME));
        sqlContains(sqlQuery, String.format("when (%s.%s > ? and %s.%s < ?)", ACCOUNT, ATTR_ACCOUNT_CITY, ACCOUNT,
                ATTR_ACCOUNT_CITY));
        sqlContains(sqlQuery, String.format("when (%s.%s>>?)&? = ? then ?", ACCOUNT, BUCKETED_PHYSICAL_ATTR));
        sqlContains(sqlQuery, "else ? end");
        sqlContains(sqlQuery, "as Score");
        sqlContains(sqlQuery, "group by Score");

    }

}
