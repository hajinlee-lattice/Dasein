package com.latticeengines.query.evaluator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.query.exposed.translator.EventQueryTranslator;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class EventQueryTranslatorTest extends QueryFunctionalTestNGBase {
    
    public static class EnhancedEventQueryTranslator extends EventQueryTranslator {
        @Override
        protected String getPeriodTransactionTableName(AttributeRepository repository) {
            return "tftest_8_periodtransaction_2018_01_06_00_57_09_utc";
        }

    }

    //private static final String PROD_ID1 = "3872223C9BA06C649D68E415E23A9446";
    //private static final String PROD_ID2 = "A78DF03BAC196BE9A08508FFDB433A31";
    private static final String PROD_ID1 = "A3B7BABBB51AD145639DD583D91826AD";
    private static final String PROD_ID2 = "563750D5B351FA4439BF5FB2A1C26DD2";

    private EventQueryTranslator getEventQueryTranslator() {
        return new EventQueryTranslator();
    }

    private TransactionRestriction getHasEngaged() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
        txRestriction.setTargetProductId(PROD_ID1);
        txRestriction.setTimeFilter(TimeFilter.ever());
        return txRestriction;
    }

    private TransactionRestriction getHasEngagedPriorToFive() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedProd1() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
        txRestriction.setTimeFilter(TimeFilter.ever());
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedProd2() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        txRestriction.setTimeFilter(TimeFilter.ever());
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getEngagedWithinSeven() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(7));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getEngagedInCurrentPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedWithin() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(6));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedWithinOverMaxPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(40));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountLessThan10K() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.SUM,
                ComparisonType.LESS_THAN,
                Collections.singletonList(1000000.0)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalQuantityGTE10Once() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    private TransactionRestriction getSumAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        txRestriction.setTargetProductId(PROD_ID2);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.SUM,
                ComparisonType.GREATER_THAN,
                Collections.singletonList(5000)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getPriorSevenEngaged() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(7));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private EventFrontEndQuery getDefaultEventFrontEndQuery() {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setPeriodName("Month");
        return frontEndQuery;
    }

    private EventFrontEndQuery getEventFrontEndQueryWithLimitedPeriodCount() {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setPeriodName("Month");
        frontEndQuery.setPeriodCount(10);
        return frontEndQuery;
    }

    private EventFrontEndQuery getEventFrontEndQueryWithProductRevenue() {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setPeriodName("Month");
        frontEndQuery.setCalculateProductRevenue(true);
        frontEndQuery.setTargetProductIds(Arrays.asList(PROD_ID1));
        return frontEndQuery;
    }

    public TransactionRestriction getSumQuantity() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        // 3872223C9BA06C649D68E415E23A9446
        txRestriction.setProductId(PROD_ID2);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.SUM,
                ComparisonType.LESS_THAN,
                Arrays.asList(20)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN,
                Arrays.asList(3000)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceAmountBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getAmountEachWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.EACH,
                ComparisonType.LESS_THAN,
                Arrays.asList(100)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getEachAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getEachAmountWithinFivePeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getAvgQuantity() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID1);
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

    public TransactionRestriction getAvgAmountInCurrentPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getTotalAmountBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        txRestriction.setTargetProductId(PROD_ID1);
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

    public TransactionRestriction getAvgAmountWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getAvgAmountBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.AVG,
                ComparisonType.LESS_OR_EQUAL,
                Arrays.asList(10)
        );
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getEachQuantityInCurrentPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getAvgQuantityWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.AVG,
                ComparisonType.LESS_OR_EQUAL,
                Arrays.asList(10.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getTotalQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.SUM,
                ComparisonType.LESS_THAN,
                Arrays.asList(100.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getAtLeastOnceQuantityWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getEachQuantityWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
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

    public TransactionRestriction getAvgQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(PROD_ID2);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                                               TimeFilter.Period.Month.name(),  //
                                               Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(
                AggregationSelector.UNIT,
                AggregationType.AVG,
                ComparisonType.LESS_THAN,
                Arrays.asList(1.0)
        );
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    @Test(groups = "functional")
    public void testLogicalOrOneChild() {
        TransactionRestriction txRestriction = getHasEngaged();
        Restriction l = Restriction.builder().or(txRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l, getDefaultEventFrontEndQuery(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalAndOneChild() {
        TransactionRestriction txRestriction = getHasEngaged();
        Restriction l = Restriction.builder().and(txRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l, getDefaultEventFrontEndQuery(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalAndTwoChildren() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalWithTwoLevels() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        TransactionRestriction t3 = getHasEngaged();
        TransactionRestriction t4 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        Restriction l2 = Restriction.builder().and(t3, t4, l1).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l2, getDefaultEventFrontEndQuery(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalWithTwoFullLevels() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        TransactionRestriction t3 = getHasEngaged();
        TransactionRestriction t4 = getSumAmount();
        Restriction l1 = Restriction.builder().or(t1, t2).build();
        Restriction l2 = Restriction.builder().or(t3, t4).build();
        Restriction l3 = Restriction.builder().and(l1, l2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l3, getDefaultEventFrontEndQuery(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testLogicalOrTwoChildren() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().or(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
    }

    @Test(groups = "functional")
    public void testHasEngaged() {
        // has engaged
        TransactionRestriction txRestriction = getHasEngaged();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 14143);
        Assert.assertEquals(count, 115);
    }

    @Test(groups = "functional")
    public void testHasNotEngaged() {
        TransactionRestriction txRestriction = getHasNotEngagedProd1();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 81921);
        Assert.assertEquals(count, 1216);
    }


    @Test(groups = "functional")
    public void testHasEngagedForTraining() {
        TransactionRestriction txRestriction = getHasEngaged();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, txRestriction,
                                                           getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 87525);
        Assert.assertEquals(count, 1857);
    }

    private TransactionRestriction getHasPurchasedLater(TransactionRestriction original,
                                                        int laggingPeriodCount) {
        TimeFilter timeFilter = new TimeFilter(original.getTimeFilter().getLhs(),
                                               ComparisonType.FOLLOWING,
                                               original.getTimeFilter().getPeriod(),
                                               Arrays.asList(laggingPeriodCount, laggingPeriodCount));


        String targetProductId = original.getTargetProductId() == null
                ? original.getProductId()
                : original.getTargetProductId();

        return new TransactionRestriction(targetProductId, //
                                          timeFilter, //
                                          false, //
                                          null, //
                                          null);
    }

    private TransactionRestriction getHasPurchasedLaterWithAmount(TransactionRestriction original,
                                                                  int laggingPeriodCount,
                                                                  int totalAmount) {
        TimeFilter timeFilter = new TimeFilter(original.getTimeFilter().getLhs(),
                                               ComparisonType.FOLLOWING,
                                               original.getTimeFilter().getPeriod(),
                                               Arrays.asList(laggingPeriodCount, laggingPeriodCount));

        String targetProductId = original.getTargetProductId() == null
                ? original.getProductId()
                : original.getTargetProductId();

        AggregationFilter spentFilter = new AggregationFilter(
                AggregationSelector.SPENT,
                AggregationType.SUM,
                ComparisonType.GREATER_THAN,
                Arrays.asList(totalAmount)
        );

        return new TransactionRestriction(targetProductId, //
                                          timeFilter, //
                                          false, //
                                          spentFilter, //
                                          null);
    }

    @Test(groups = "functional")
    public void testFirstPurchaseForEvent() {
        TransactionRestriction txRestriction = getHasNotEngagedProd1();
        TransactionRestriction nextRestriction = getHasPurchasedLaterWithAmount(txRestriction, 1, 200);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                                                        getDefaultEventFrontEndQuery(),
                                                        Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        Assert.assertEquals(count, 52);
    }

    @Test(groups = "functional")
    public void testHasEngagedForEvent() {
        TransactionRestriction txRestriction = getHasEngaged();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                                                        getDefaultEventFrontEndQuery(),
                                                        Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 17564);
        Assert.assertEquals(count, 318);
    }

    @Test(groups = "functional")
    public void testHasEngagedForEventWithRevenue() throws SQLException {
        TransactionRestriction txRestriction = getHasEngaged();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                                                        getEventFrontEndQueryWithProductRevenue(),
                                                        Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        ResultSet result = sqlQuery.getResults();

        //Assert.assertEquals(count, 17564);
        ResultSetMetaData metaData = result.getMetaData();
        Assert.assertEquals(metaData.getColumnCount(), 3);
        int count = 0;
        while (result.next()) {
            Assert.assertTrue(result.getInt("revenue") > 0);
            count += 1;
        }
        Assert.assertEquals(count, 318);
    }

    @Test(groups = "functional")
    public void testHasEngagedPrior() {
        // has engaged
        TransactionRestriction txRestriction = getHasEngagedPriorToFive();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 5189);
        Assert.assertEquals(count, 50);
    }

    @Test(groups = "functional")
    public void testHasPurchasedInCurrentPeriod() {
        TransactionRestriction txRestriction = getEngagedInCurrentPeriod();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 4565);
        Assert.assertEquals(count, 18);
    }

    @Test(groups = "functional")
    public void testHasNotPurchasedWithin() {
        TransactionRestriction txRestriction = getHasNotPurchasedWithin();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 86093);
        Assert.assertEquals(count, 1265);
    }

    @Test(groups = "functional")
    public void testHasNotPurchasedWithinOverMaxPeriod() {
        TransactionRestriction txRestriction = getHasNotPurchasedWithinOverMaxPeriod();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 0);
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "functional")
    public void testLogicalAndTwoChildrenForTraining() {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                                                           Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 4595);
        Assert.assertEquals(count, 9);
    }

    @Test(groups = "functional")
    public void testLogicalAndTwoChildrenForEvent() {
        TransactionRestriction txRestriction = getHasEngaged();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction t1 = Restriction.builder().and(txRestriction, nextRestriction).build();
        TransactionRestriction sumRestriction = getSumAmount();
        TransactionRestriction sumNextRestriction = getHasPurchasedLater(sumRestriction, 1);
        Restriction t2 = Restriction.builder().and(sumRestriction, sumNextRestriction).build();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                                                        Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 2317);
        Assert.assertEquals(count, 1);
    }


    @Test(groups = "functional")
    public void testTotalAmountLessThan10K() {
        TransactionRestriction txRestriction = getTotalAmountLessThan10K();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 96058);
        Assert.assertEquals(count, 1331);

    }

    @Test(groups = "functional")
    public void testQuantityAtLeastOnce() {
        TransactionRestriction txRestriction = getTotalQuantityGTE10Once();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 6698);
        Assert.assertEquals(count, 42);

    }

    @Test(groups = "functional")
    public void testTotalQuantity() {
        TransactionRestriction txRestriction = getSumQuantity();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        //System.out.println("sqlQuery = " + sqlQuery);

    }

    @Test(groups = "functional")
    public void testAmountAtLeastOnce() {
        TransactionRestriction txRestriction = getAtLeastOnceAmount();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);

    }

    @Test(groups = "functional")
    public void testAmountAtLeastOnceBetweenPeriods() {
        TransactionRestriction txRestriction = getAtLeastOnceAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 2178);
        Assert.assertEquals(count, 7);
    }

    @Test(groups = "functional")
    public void testEachAmountEver() {
        TransactionRestriction txRestriction = getEachAmount();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 535);
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "functional")
    public void testEachAmountWithinFivePeriods() {
        TransactionRestriction txRestriction = getEachAmountWithinFivePeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 928);
        Assert.assertEquals(count, 2);
    }

    @Test(groups = "functional")
    public void testAvgQuantityEver() {
        TransactionRestriction txRestriction = getAvgQuantity();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 2610);
        Assert.assertEquals(count, 16);
    }

    @Test(groups = "functional")
    public void testAvgAmountInCurrentPeriod() {
        TransactionRestriction txRestriction = getAvgAmountInCurrentPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 3363);
        Assert.assertEquals(count, 15);
    }

    @Test(groups = "functional")
    public void testTotalAmountBetweenPeriodsForTraining() {
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, txRestriction,
                                                           getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 2329);
        Assert.assertEquals(count, 21);
    }

    @Test(groups = "functional")
    public void testTotalAmountBetweenPeriodsForEvent() {
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                                                        getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 707);
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "functional")
    public void testTotalAmountBetweenPeriods() {
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 883);
        Assert.assertEquals(count, 1);
    }

    @Test(groups = "functional")
    public void testAvgAmountWithinPeriods() {
        TransactionRestriction txRestriction = getAvgAmountWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 736);
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "functional")
    public void testAmountEachLessThanWithinPeriod() {
        TransactionRestriction amount = getAmountEachWithinPeriod();
        TransactionRestriction hasNotEngaged = getHasNotEngagedWithinPeriod();
        Restriction logicalRestriction = Restriction.builder().or(amount, hasNotEngaged).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, logicalRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 85983);
        Assert.assertEquals(count, 1277);
    }

    @Test(groups = "functional")
    public void testAvgAmountBetweenPeriods() {
        TransactionRestriction txRestriction = getAvgAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 54661);
        Assert.assertEquals(count, 1180);
    }

    @Test(groups = "functional")
    public void testEachQuantityInCurrentPeriod() {
        TransactionRestriction txRestriction = getEachQuantityInCurrentPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 1194);
        Assert.assertEquals(count, 5);
    }

    @Test(groups = "functional")
    public void testAvgQuantityWithinPeriod() {
        TransactionRestriction txRestriction = getAvgQuantityWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 94855);
        Assert.assertEquals(count, 1326);
    }

    @Test(groups = "functional")
    public void testTotalQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = getTotalQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 76796);
        Assert.assertEquals(count, 1290);
    }

    @Test(groups = "functional")
    public void testAtLeastOnceQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = getAtLeastOnceQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 797);
        Assert.assertEquals(count, 1);
    }

    @Test(groups = "functional")
    public void testAtLeastOnceQuantityWithinPeriod() {
        TransactionRestriction txRestriction = getAtLeastOnceQuantityWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 408);
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "functional")
    public void testEachQuantityWithinPeriod() {
        TransactionRestriction txRestriction = getEachQuantityWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 928);
        Assert.assertEquals(count, 2);
    }

    @Test(groups = "functional")
    public void testAvgQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = getAvgQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 57807);
        Assert.assertEquals(count, 1219);
    }

    @Test(groups = "functional")
    public void testAvgQuantityBetweenPeriodsWithLimitedPeriods() {
        TransactionRestriction txRestriction = getAvgQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getEventFrontEndQueryWithLimitedPeriodCount(),
                                                          Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 57807);
        Assert.assertEquals(count, 783);
    }

    @Test(groups = "functional")
    public void testPriorOnly() {
        TransactionRestriction txRestriction = getPriorSevenEngaged();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 4298);
        Assert.assertEquals(count, 42);

    }

    @Test(groups = "functional")
    public void testPriorOnlyNegativeCase() {
        EventQueryTranslator eventTranslator = getEventQueryTranslator();

        TransactionRestriction txRestriction = getPriorSevenEngaged();
        txRestriction.setNegate(true);
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 91766);
        Assert.assertEquals(count, 1289);

        TransactionRestriction hasNotEngaged = getHasNotEngagedProd2();
        TransactionRestriction within = getEngagedWithinSeven();
        Restriction logicalRestriction = Restriction.builder().or(hasNotEngaged, within).build();
        Query query1 = eventTranslator.translateForScoring(queryFactory, attrRepo, logicalRestriction,
                                                           getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery1 = queryEvaluator.evaluate(attrRepo, query1, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery1);
        long count1 = queryEvaluatorService.getCount(attrRepo, query1, SQL_USER);
        //Assert.assertEquals(count1, 91766);
        Assert.assertEquals(count1, 1289);

    }

    @Test(groups = "functional")
    public void testPriorOnlyTwoLevel() {
        TransactionRestriction priorOnlyRestriction = getPriorSevenEngaged();
        TransactionRestriction sumAmountRestriction = getSumAmount();
        Restriction priorAndSumRestriction =
                Restriction.builder().or(sumAmountRestriction, priorOnlyRestriction).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, priorAndSumRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = queryEvaluatorService.getCount(attrRepo, query, SQL_USER);
        //Assert.assertEquals(count, 5743);
        Assert.assertEquals(count, 50);

    }

    @Test(groups = "functional")
    public void testSimpleAccountQuery() {
        Restriction acctRestriction = Restriction.builder()
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq("0012400001DNKKLAA5").build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, acctRestriction,
                                                          getDefaultEventFrontEndQuery(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = sqlQuery.fetchCount();
        Assert.assertEquals(count, 1);

    }

    @Test(groups = "functional")
    public void testAccountEventQuery() {
        Restriction acctRestriction = Restriction.builder()
                .let(BusinessEntity.Account, ATTR_ACCOUNT_ID).eq("0012400001DNL2vAAH").build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, acctRestriction,
                                                        getEventFrontEndQueryWithProductRevenue(), Query.builder(), SQL_USER).build();
        SQLQuery sqlQuery = queryEvaluator.evaluate(attrRepo, query, SQL_USER);
        System.out.println("sqlQuery = " + sqlQuery);
        long count = sqlQuery.fetchCount();
        Assert.assertEquals(count, 26);

    }

}
