package com.latticeengines.query.evaluator;

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.query.exposed.translator.EventQueryTranslator;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class EventQueryTranslatorMultiProductTest extends QueryFunctionalTestNGBase {

    public static class EnhancedEventQueryTranslator extends EventQueryTranslator {
        @Override
        protected String getPeriodTransactionTableName(AttributeRepository repository) {
            return "tftest_8_periodtransaction_2018_01_06_00_57_09_utc";
        }

    }

    private EventQueryTranslator getEventQueryTranslator() {
        return new EnhancedEventQueryTranslator();
    }

    private TransactionRestriction getHasEngagedPriorToFive(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedProd1(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedProd2(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getEngagedWithinSeven(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(7));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getEngagedInCurrentPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedWithin(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(6));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountLessThan1M(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
            AggregationSelector.SPENT,
            AggregationType.SUM,
            ComparisonType.LESS_THAN,
            Collections.singletonList(1000000.0),
            true
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalQuantityGTE10Once(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(10.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getPriorSevenEngaged(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(7));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private EventFrontEndQuery getDefaultEventFrontEndQuery() {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setPeriodName("Month");
        frontEndQuery.setEvaluationPeriodId(10);
        return frontEndQuery;
    }

    public TransactionRestriction getAtLeastOnceAmountBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN,
                Arrays.asList(1000)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAmountEachWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
            AggregationSelector.SPENT,
            AggregationType.EACH,
            ComparisonType.LESS_THAN,
            Arrays.asList(100),
            true
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachAmount(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.EACH,
                ComparisonType.GREATER_THAN,
                Arrays.asList(0)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachAmountWithinFivePeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.EACH,
                ComparisonType.GREATER_THAN,
                Arrays.asList(0)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgQuantity(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.AVG,
                ComparisonType.GREATER_OR_EQUAL,
                Arrays.asList(1.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgAmountInCurrentPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        //txRestriction.setProductId(PROD_ID2);
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.AVG,
                ComparisonType.GREATER_OR_EQUAL,
                Arrays.asList(100.0)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.SUM,
                ComparisonType.GREATER_THAN,
                Arrays.asList(5000.0)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgAmountWithinPeriod(String prodIdList) {
        // XXX
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.AVG,
                ComparisonType.GREATER_THAN,
                Arrays.asList(1000.0)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgAmountBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
            AggregationSelector.SPENT,
            AggregationType.AVG,
            ComparisonType.LESS_OR_EQUAL,
            Arrays.asList(10),
            true
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachQuantityInCurrentPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.EACH,
                ComparisonType.GREATER_THAN,
                Arrays.asList(10.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgQuantityWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
            AggregationSelector.UNIT,
            AggregationType.AVG,
            ComparisonType.LESS_OR_EQUAL,
            Arrays.asList(10.0),
            true
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalQuantityBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
            AggregationSelector.UNIT,
            AggregationType.SUM,
            ComparisonType.LESS_THAN,
            Arrays.asList(100.0),
            true
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAtLeastOnceQuantityBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN,
                Arrays.asList(50.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAtLeastOnceQuantityWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_OR_EQUAL,
                Arrays.asList(100.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachQuantityWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.EACH,
                ComparisonType.GREATER_THAN,
                Arrays.asList(0.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgQuantityBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
            AggregationSelector.UNIT,
            AggregationType.AVG,
            ComparisonType.LESS_THAN,
            Arrays.asList(1.0),
            true
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }


    @Test(groups = "functional")
    public void testHasEngagedPrior() {
        // DS_Test_1
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,6B047FF03A59E42A79D1961541C1BF60,FE4D73162629DF254688CC8D553FA52A";
        TransactionRestriction txRestriction = getHasEngagedPriorToFive(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 5966);
    }

    @Test(groups = "functional")
    public void testHasPurchasedInCurrentPeriod() {
        // DS_Test_2
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,6B047FF03A59E42A79D1961541C1BF60,FE4D73162629DF254688CC8D553FA52A";
        TransactionRestriction txRestriction = getEngagedInCurrentPeriod(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 2862);
    }

    @Test(groups = "functional")
    public void testHasNotPurchasedWithin() {
        // DS_Test_3
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,3CA17E482511F94FCEB93195D2B146DE,94842E58561D516577E4C0377F601DA3";
        TransactionRestriction txRestriction = getHasNotPurchasedWithin(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 76987);
    }

    //@Test(groups = "functional")
    public void testHasNotEngaged() {
        // DS_Test_4
        String prodIdList = "tbd";
        TransactionRestriction txRestriction = getHasNotEngagedProd1(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 68680);
    }

    @Test(groups = "functional")
    public void testPriorOnlyNegativeCase() {
        // DS_Test_5
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,8D6A468D2B5F99D5D2252DE0DF6F971C,A78DF03BAC196BE9A08508FFDB433A31";
        EventQueryTranslator eventTranslator = getEventQueryTranslator();

        TransactionRestriction txRestriction = getPriorSevenEngaged(prodIdList);
        txRestriction.setNegate(true);
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 88959);
    }

    @Test(groups = "functional")
    public void testTotalAmountLessThan1M() {
        // DS_Test_6
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,40BE92E2D8ADE18DC80A3FAEC761F91A,D7B8185BB08C1AFA624B7B9BA49AA77C,6C5E5F2F07EB909FDB480F868047586C";
        TransactionRestriction txRestriction = getTotalAmountLessThan1M(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 96058);
    }

    @Test(groups = "functional")
    public void testEachAmountEver() {
        // DS_Test_7
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,64E8729DCA85D713B8BE424E6A2828E5,EBB5D5D47420FE96255F586D37FE3EB0";
        TransactionRestriction txRestriction = getEachAmount(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 210);
    }

    @Test(groups = "functional")
    public void testAvgQuantityEver() {
        // DS_Test_8
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,56E3D7F5C674E7AC8026A61360A80769";
        TransactionRestriction txRestriction = getAvgQuantity(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 13985);
    }

    @Test(groups = "functional")
    public void testQuantityAtLeastOnce() {
        // DS_Test_9
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,B365E24EC8F2C3672684ACC2F2EE208A";
        TransactionRestriction txRestriction = getTotalQuantityGTE10Once(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 1196);

    }

    @Test(groups = "functional")
    public void testAvgAmountInCurrentPeriod() {
        // DS_Test_10
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,8D6A468D2B5F99D5D2252DE0DF6F971C,D45763A3E784654F2C17CF906F9CD295";
        TransactionRestriction txRestriction = getAvgAmountInCurrentPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 3795);
    }

    @Test(groups = "functional")
    public void testEachQuantityWithinPeriod() {
        // DS_Test_11
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,7D18DD009AC1004A4E72B66E2D8F594F,5544C27D9C475BF2B639241F65D8D26F";
        TransactionRestriction txRestriction = getEachQuantityWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 318);
    }

    @Test(groups = "functional")
    public void testAmountAtLeastOnceBetweenPeriods() {
        // DS_Test_12
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,E49000DA92C149E7BF960306185BF577,36E2AF3FC4AE03328EE407C3A3933007";
        TransactionRestriction txRestriction = getAtLeastOnceAmountBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 6023);
    }

    @Test(groups = "functional")
    public void testTotalAmountBetweenPeriods() {
        // DS_Test_13
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,1EF99C5568D3047FBD9D4A271614B008,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 4464);
    }

    @Test(groups = "functional")
    public void testAvgAmountWithinPeriods() {
        // DS_Test_14
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getAvgAmountWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 2881);
    }

    @Test(groups = "functional")
    public void testAmountEachLessThanWithinPeriod() {
        // DS_Test_15
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction amount = getAmountEachWithinPeriod(prodIdList);
        TransactionRestriction hasNotEngaged = getHasNotEngagedWithinPeriod(prodIdList);
        Restriction logicalRestriction = Restriction.builder().or(amount, hasNotEngaged).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, logicalRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 85989);
    }

    @Test(groups = "functional")
    public void testAvgAmountBetweenPeriods() {
        // DS_Test_16
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,94842E58561D516577E4C0377F601DA3,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getAvgAmountBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 82723);
    }

    @Test(groups = "functional")
    public void testEachQuantityInCurrentPeriod() {
        // DS_Test_17
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,94842E58561D516577E4C0377F601DA3,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getEachQuantityInCurrentPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 776);
    }

    @Test(groups = "functional")
    public void testAvgQuantityWithinPeriod() {
        // DS_Test_18
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getAvgQuantityWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 95895);
    }

    @Test(groups = "functional")
    public void testTotalQuantityBetweenPeriods() {
        // DS_Test_19
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getTotalQuantityBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 95949);
    }

    @Test(groups = "functional")
    public void testAtLeastOnceQuantityBetweenPeriods() {
        // DS_Test_20
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getAtLeastOnceQuantityBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 97);
    }

    @Test(groups = "functional")
    public void testAtLeastOnceQuantityWithinPeriod() {
        // DS_Test_21
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,94842E58561D516577E4C0377F601DA3,A78DF03BAC196BE9A08508FFDB433A31";
        TransactionRestriction txRestriction = getAtLeastOnceQuantityWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 596);
    }

    @Test(groups = "functional")
    public void testEachAmountWithinFivePeriods() {
        // DS_Test_22
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,CFF528B14A522C97F5F23964583EDB78";
        TransactionRestriction txRestriction = getEachAmountWithinFivePeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 317);
    }

    @Test(groups = "functional")
    public void testAvgQuantityBetweenPeriods() {
        // DS_Test_23
        String prodIdList = "3872223C9BA06C649D68E415E23A9446,2A2A5856EC1CCB78E786DF65564DA39E,94842E58561D516577E4C0377F601DA3";
        TransactionRestriction txRestriction = getAvgQuantityBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 89763);
    }

}
