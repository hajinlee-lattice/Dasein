package com.latticeengines.query.evaluator;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
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
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.query.exposed.translator.NewTransactionRestrictionTranslator;
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
        Assert.assertEquals(count, 5);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .notcontains(accountId.substring(0, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, TOTAL_RECORDS - 5);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).eq(accountId) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 13);

        restriction = Restriction.builder() //
                .let(BusinessEntity.Contact, ATTR_ACCOUNT_ID).contains(accountId.substring(1, accountId.length() - 1)) //
                .build();
        query = Query.builder() //
                .find(BusinessEntity.Contact) //
                .where(restriction) //
                .build();
        count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 13);
    }

    @Test(groups = "functional", enabled = true)
    public void testTransactionSelect() throws ParseException {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId("0802DD00110356F3289420FE46850008");
        QueryBuilder builder = Query.builder();
        Restriction restriction = new NewTransactionRestrictionTranslator().convert(txRestriction,
                                                                                    queryFactory, attrRepo,
                                                                                    BusinessEntity.Account,
                                                                                    builder);
        Restriction cityRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_CITY)
                .eq("Richland").build();
        Restriction idRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .eq("0012400001DO2QKAA1").build();
        Restriction cityAndTx = Restriction.builder().and(cityRestriction, restriction).build();
        Restriction idOrCityAndTx = Restriction.builder().or(idRestriction, cityAndTx).build();
        Query query = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(idOrCityAndTx).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 2);
    }

    @Test(groups = "functional", enabled = true)
    public void testTransactionSelectEver() throws ParseException {
        // total spent > 3000
        TransactionRestriction txRestrictionSpentSum = new TransactionRestriction();
        txRestrictionSpentSum.setProductId("E986FA1C1503DCB38A95CF92F3977E34");
        txRestrictionSpentSum.setTimeFilter(TimeFilter.ever());
        txRestrictionSpentSum.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Collections.singletonList(3000)));

        QueryBuilder builder = Query.builder();
        Restriction restrictionSpentSum = new NewTransactionRestrictionTranslator().convert(txRestrictionSpentSum,
                                                                                            queryFactory, attrRepo,
                                                                                            BusinessEntity.Account,
                                                                                            builder);
        Restriction accountIdRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .eq("0012400001DNJYKAA5").build();
        Restriction accountAndTxSpentSum = Restriction.builder().and(accountIdRestriction, restrictionSpentSum).build();
        Query querySpentSum = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(accountAndTxSpentSum).build();
        List<Map<String, Object>> spentSumResults = queryEvaluatorService.getData(attrRepo, querySpentSum).getData();
        Assert.assertEquals(spentSumResults.size(), 0);

        // period spent >= 200 in each period
        builder = Query.builder();
        TransactionRestriction txRestrictionSpentEach = new TransactionRestriction();
        txRestrictionSpentEach.setProductId("E986FA1C1503DCB38A95CF92F3977E34");
        txRestrictionSpentEach.setTimeFilter(TimeFilter.ever());
        txRestrictionSpentEach.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.EACH,
                ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(200)));
        Restriction restrictionSpentEach = new NewTransactionRestrictionTranslator().convert(txRestrictionSpentEach,
                                                                                             queryFactory, attrRepo,
                                                                                             BusinessEntity.Account,
                                                                                             builder);
        Restriction accountAndTxSpentEach = Restriction.builder().and(accountIdRestriction, restrictionSpentEach)
                .build();
        Query querySpentEach = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(accountAndTxSpentEach).build();
        List<Map<String, Object>> spentEachResults = queryEvaluatorService.getData(attrRepo, querySpentEach).getData();
        Assert.assertEquals(spentEachResults.size(), 0);

        // period spent >= 2900 at least once
        builder = Query.builder();
        TransactionRestriction txRestrictionSpentOnce = new TransactionRestriction();
        txRestrictionSpentOnce.setProductId("E986FA1C1503DCB38A95CF92F3977E34");
        txRestrictionSpentOnce.setTimeFilter(TimeFilter.ever());
        txRestrictionSpentOnce.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.AT_LEAST_ONCE, ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(2900)));
        Restriction restrictionSpentOnce = new NewTransactionRestrictionTranslator().convert(txRestrictionSpentOnce,
                                                                                             queryFactory, attrRepo,
                                                                                             BusinessEntity.Account,
                                                                                             builder);
        Restriction accountAndTxSpentOnce = Restriction.builder().and(accountIdRestriction, restrictionSpentOnce)
                .build();
        Query querySpentOnce = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(accountAndTxSpentOnce).build();
        List<Map<String, Object>> spentOnceResults = queryEvaluatorService.getData(attrRepo, querySpentOnce).getData();
        Assert.assertEquals(spentOnceResults.size(), 0);

        // total unit > 20 (sum total is 18)
        builder = Query.builder();
        TransactionRestriction txRestrictionUnitSum = new TransactionRestriction();
        txRestrictionUnitSum.setProductId("E986FA1C1503DCB38A95CF92F3977E34");
        txRestrictionUnitSum.setTimeFilter(TimeFilter.ever());
        txRestrictionUnitSum.setUnitFilter(new AggregationFilter(AggregationSelector.UNIT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Collections.singletonList(20)));
        Restriction restrictionUnitSum = new NewTransactionRestrictionTranslator().convert(txRestrictionUnitSum,
                                                                                           queryFactory, attrRepo,
                                                                                           BusinessEntity.Account,
                                                                                           builder);
        Restriction accountAndTxUnitSum = Restriction.builder().and(accountIdRestriction, restrictionUnitSum).build();
        Query queryUnitSum = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(accountAndTxUnitSum).build();
        List<Map<String, Object>> unitSumResults = queryEvaluatorService.getData(attrRepo, queryUnitSum).getData();
        Assert.assertEquals(unitSumResults.size(), 0);

        // avg unit = 2
        builder = Query.builder();
        TransactionRestriction txRestrictionUnitAvg = new TransactionRestriction();
        txRestrictionUnitAvg.setProductId("E986FA1C1503DCB38A95CF92F3977E34");
        txRestrictionUnitAvg.setTimeFilter(TimeFilter.ever());
        txRestrictionUnitAvg.setUnitFilter(new AggregationFilter(AggregationSelector.UNIT, AggregationType.AVG,
                ComparisonType.EQUAL, Collections.singletonList(2)));
        Restriction restrictionUnitAvg = new NewTransactionRestrictionTranslator().convert(txRestrictionUnitAvg,
                                                                                           queryFactory, attrRepo,
                                                                                           BusinessEntity.Account,
                                                                                           builder);
        Restriction accountAndTxUnitAvg = Restriction.builder().and(accountIdRestriction, restrictionUnitAvg).build();
        Query queryUnitAvg = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(accountAndTxUnitAvg).build();
        List<Map<String, Object>> unitAvgResults = queryEvaluatorService.getData(attrRepo, queryUnitAvg).getData();
        Assert.assertEquals(unitAvgResults.size(), 0);
    }

    @Test(groups = "functional")
    public void testTimeFilter() {
        // This query actually returns all accounts so it doesn't change over time
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Transaction, TRS_TRANSACTION_DATE)//
                .prior(Period.Quarter, 1) //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Transaction, ATTR_ACCOUNT_ID) //
                .where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, 108045);
    }

    @Test(groups = "functional")
    public void testTransactionSelectWithTimeFilter() throws ParseException {
        // adjust period offset based on current date
        LocalDate periodStart = LocalDate.of(2017, Month.APRIL, 1);
        LocalDate currentDate = LocalDate.now();
        long diffInMonths = ChronoUnit.MONTHS.between(periodStart, currentDate);
        long periodOffset = diffInMonths / 3;

        // period spent > 200 each period prior to quarter of 2017/4/1
        QueryBuilder builder = Query.builder();
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction.setTimeFilter(
                new TimeFilter(ComparisonType.PRIOR, Period.Quarter, Arrays.asList(new Object[] { periodOffset })));
        txRestriction.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.EACH,
                ComparisonType.GREATER_THAN, Collections.singletonList(200)));
        Restriction restriction = new NewTransactionRestrictionTranslator().convert(txRestriction,
                                                                                    queryFactory, attrRepo,
                                                                                    BusinessEntity.Account,
                                                                                    builder);
        Query query = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction).build();
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 1);

        // period spent > 200 at_least_once within quarter of 2017/4/1
        builder = Query.builder();
        TransactionRestriction txRestriction1 = new TransactionRestriction();
        txRestriction1.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction1.setTimeFilter(
                new TimeFilter(ComparisonType.WITHIN, Period.Quarter, Arrays.asList(new Object[] { periodOffset })));
        txRestriction1.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Collections.singletonList(200)));
        Restriction restriction1 = new NewTransactionRestrictionTranslator().convert(txRestriction1,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Query query1 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction1).build();
        List<Map<String, Object>> results1 = queryEvaluatorService.getData(attrRepo, query1).getData();
        Assert.assertEquals(results1.size(), 49);

        // period spent > 200 between [0, quarter of 2017/4/1], should get the same
        // result as above
        builder = Query.builder();
        TransactionRestriction txRestriction2 = new TransactionRestriction();
        txRestriction2.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction2.setTimeFilter(new TimeFilter(ComparisonType.BETWEEN, Period.Quarter,
                Arrays.asList(new Object[] { 0, periodOffset })));
        txRestriction2.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Collections.singletonList(200)));
        Restriction restriction2 = new NewTransactionRestrictionTranslator().convert(txRestriction2,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Query query2 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction2).build();
        List<Map<String, Object>> results2 = queryEvaluatorService.getData(attrRepo, query2).getData();
        Assert.assertEquals(results2.size(), 49);

        // no qualified account in current period
        builder = Query.builder();
        TransactionRestriction txRestriction3 = new TransactionRestriction();
        txRestriction3.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction3.setTimeFilter(
                new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, Period.Quarter, Arrays.asList(new Object[] { 0 })));
        txRestriction3.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Collections.singletonList(200)));
        Restriction restriction3 = new NewTransactionRestrictionTranslator().convert(txRestriction3,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Query query3 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction3).build();
        List<Map<String, Object>> results3 = queryEvaluatorService.getData(attrRepo, query3).getData();
        Assert.assertEquals(results3.size(), 0);

        // at_least_once period spent > 200 with period eq quarter of 4/1/2017
        builder = Query.builder();
        TransactionRestriction txRestriction4 = new TransactionRestriction();
        txRestriction4.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction4.setTimeFilter(
                new TimeFilter(ComparisonType.EQUAL, Period.Quarter, Arrays.asList(new Object[] { periodOffset })));
        txRestriction4.setSpentFilter(new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Collections.singletonList(200)));
        Restriction restriction4 = new NewTransactionRestrictionTranslator().convert(txRestriction4,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Query query4 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction4).build();
        List<Map<String, Object>> results4 = queryEvaluatorService.getData(attrRepo, query4).getData();
        Assert.assertEquals(results4.size(), 37);

        // has purchased prior to quarter of 4/1/2017, but not within
        builder = Query.builder();
        TransactionRestriction txRestriction5 = new TransactionRestriction();
        txRestriction5.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction5.setTimeFilter(
                new TimeFilter(ComparisonType.PRIOR, Period.Quarter, Arrays.asList(new Object[] { periodOffset })));
        TransactionRestriction txRestriction6 = new TransactionRestriction();
        txRestriction6.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction6.setTimeFilter(
                new TimeFilter(ComparisonType.WITHIN, Period.Quarter, Arrays.asList(new Object[] { periodOffset })));
        txRestriction6.setNegate(true);
        Restriction restriction5 = new NewTransactionRestrictionTranslator().convert(txRestriction5,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Restriction restriction6 = new NewTransactionRestrictionTranslator().convert(txRestriction6,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Restriction priorToLastRestriction = Restriction.builder().and(restriction5, restriction6).build();
        Query query5 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(priorToLastRestriction).build();
        List<Map<String, Object>> results5 = queryEvaluatorService.getData(attrRepo, query5).getData();
        Assert.assertEquals(results5.size(), 171);

        // prior_to_last, should be the same as previous result
        builder = Query.builder();
        TransactionRestriction txRestriction7 = new TransactionRestriction();
        txRestriction7.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction7.setTimeFilter(new TimeFilter(ComparisonType.PRIOR_ONLY, Period.Quarter,
                                                    Arrays.asList(new Object[]{periodOffset})));
        Restriction restriction7 = new NewTransactionRestrictionTranslator().convert(txRestriction7,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Query query7 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .where(restriction7).build();
        List<Map<String, Object>> results7 = queryEvaluatorService.getData(attrRepo, query7).getData();
        Assert.assertEquals(results7.size(), 171);

        builder = Query.builder();
        TransactionRestriction txRestriction8 = new TransactionRestriction();
        txRestriction8.setProductId("0720FE59CDE6B915173E381A517876B7");
        txRestriction8.setTimeFilter(new TimeFilter(ComparisonType.PRIOR_ONLY, Period.Quarter,
                                                    Arrays.asList(new Object[]{periodOffset})));
        txRestriction8.setNegate(true);
        Restriction restriction8 = new NewTransactionRestrictionTranslator().convert(txRestriction8,
                                                                                     queryFactory, attrRepo,
                                                                                     BusinessEntity.Account,
                                                                                     builder);
        Query query8 = builder //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID) //
                .from(BusinessEntity.Account).where(restriction8).build();
        List<Map<String, Object>> results8 = queryEvaluatorService.getData(attrRepo, query8).getData();
        Assert.assertEquals(results8.size(), 105173);
    }

    @Test(groups = "functional")
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

            Assert.assertEquals(row.get("LDC_Name").toString(), "Lake Region Medical, Inc.");
            Assert.assertEquals(row.get("LDC_State").toString().toUpperCase(), "MASSACHUSETTS");
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
        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 2);
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("A", "Z") //
                .build();
        Query query1 = Query.builder().where(range1).build();
        long count1 = queryEvaluatorService.getCount(attrRepo, query1);
        Assert.assertEquals(count1, 104888);

        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.Account, "AlexaViewsPerUser").in(1.0, 9.5) //
                .build();
        Query query2 = Query.builder().where(range2).build();
        long count2 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count2, 46447);

        query2 = Query.builder().where(range2).from(BusinessEntity.Account).build();
        count2 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count2, 46447);

        query2 = Query.builder().where(range2).from(BusinessEntity.Contact).build();
        long count3 = queryEvaluatorService.getCount(attrRepo, query2);
        Assert.assertEquals(count3, 3130);

        Restriction restriction = Restriction.builder().and(range1, range2).build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertTrue(count <= count1 && count <= count2);
        Assert.assertEquals(count, 46231);
    }

    @Test(groups = "functional", dataProvider = "bitEncodedData")
    public void testBitEncoded(ComparisonType operator, String value, long expectedCount) {
        // bucket
        RestrictionBuilder builder = Restriction.builder();
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
            case CONTAINS:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).contains(value);
                break;
            case IS_NULL:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNull();
                break;
            case IS_NOT_NULL:
                builder = builder.let(BusinessEntity.Account, BUCKETED_NOMINAL_ATTR).isNotNull();
                break;
            default:
                throw new UnsupportedOperationException("Does not support " + operator);
        }
        Restriction restriction = builder.build();
        Query query = Query.builder().where(restriction).build();
        long count = queryEvaluatorService.getCount(attrRepo, query);
        Assert.assertEquals(count, expectedCount);
    }

    @DataProvider(name = "bitEncodedData", parallel = true)
    private Object[][] provideBitEncodedData() {
        return new Object[][] {
                { ComparisonType.EQUAL, "Yes", BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.EQUAL, "No", BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.EQUAL, null, BUCKETED_NULL_IN_CUSTOEMR }, //

                { ComparisonType.IS_NULL, null, BUCKETED_NULL_IN_CUSTOEMR }, //
                { ComparisonType.IS_NOT_NULL, null, BUCKETED_YES_IN_CUSTOEMR + BUCKETED_NO_IN_CUSTOEMR }, //

                { ComparisonType.NOT_EQUAL, "Yes", BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.NOT_EQUAL, "No", BUCKETED_YES_IN_CUSTOEMR }, //

                { ComparisonType.STARTS_WITH, "y", BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.STARTS_WITH, "N", BUCKETED_NO_IN_CUSTOEMR }, //
                { ComparisonType.CONTAINS, "e", BUCKETED_YES_IN_CUSTOEMR }, //
                { ComparisonType.CONTAINS, "o", BUCKETED_NO_IN_CUSTOEMR }, //
        };
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction domainInRange = Restriction.builder() //
                .let(BusinessEntity.Account, ATTR_ACCOUNT_CITY).in("CA", "CZ") //
                .build();
        Query query1 = Query.builder()
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(domainInRange) //
                .orderBy(BusinessEntity.Account, ATTR_ACCOUNT_NAME) //
                .build();
        long countInRedshift = queryEvaluatorService.getCount(attrRepo, query1);
        Assert.assertEquals(countInRedshift, 56);

        List<Map<String, Object>> results;
        int offset = 0;
        int pageSize = 20;
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
                .let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("A", "H") //
                .build();

        Query query = Query.builder() //
                .select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .build();

        List<Map<String, Object>> results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), 40783);
        long count = results.stream().filter(result -> {
            String city = (String) result.get(ATTR_ACCOUNT_CITY);
            return city != null && city.toUpperCase().contains("HAM");
        }).count();
        Assert.assertEquals(count, 335);

        query = Query.builder().select(BusinessEntity.Account, ATTR_ACCOUNT_ID, ATTR_ACCOUNT_NAME, ATTR_ACCOUNT_CITY) //
                .where(nameInRange) //
                .freeText("ham", BusinessEntity.Account, ATTR_ACCOUNT_CITY) //
                .build();

        results = queryEvaluatorService.getData(attrRepo, query).getData();
        Assert.assertEquals(results.size(), count);
    }

    @Test(groups = "functional")
    public void testCaseLookup() {
        TreeMap<String, Restriction> cases = new TreeMap<>();
        Restriction A = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_NAME).in("C", "D").build();
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
    }

}
