package com.latticeengines.query.evaluator;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
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

    private static Logger log = LoggerFactory.getLogger(EventQueryTranslatorTest.class);

    // private static final String PROD_ID1 =
    // "3872223C9BA06C649D68E415E23A9446";
    // private static final String PROD_ID2 =
    // "A78DF03BAC196BE9A08508FFDB433A31";
    private static final String PROD_ID1 = "A3B7BABBB51AD145639DD583D91826AD";
    private static final String PROD_ID2 = "563750D5B351FA4439BF5FB2A1C26DD2";

    private EventQueryTranslator getEventQueryTranslator() {
        return new EventQueryTranslator();
    }

    protected String getProductId1() {
        return PROD_ID1;
    }

    protected String getProductId2() {
        return PROD_ID2;
    }

    protected String getAccountId1() {
        return "0012400001DNKKLAA5";
    }

    protected String getAccountId2() {
        return "0012400001DNL2vAAH";
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] { { SQL_USER, "Redshift" } };
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod(Method method, Object[] params) {
        System.out.println(String.format("\n*********** Running Test Method: %s, Params: %s **********",
                method.getName(), Arrays.toString(params)));
    }

    @AfterMethod(groups = "functional")
    public void afterMethod(ITestResult testResult, Object[] params) {
        System.out.println(String.format("---------- Completed Test Method: %s, Params: %s, Time: %d ----------\n",
                testResult.getMethod().getMethodName(), Arrays.toString(params),
                (testResult.getEndMillis() - testResult.getStartMillis())));
    }

    private TransactionRestriction getHasEngaged() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        txRestriction.setTargetProductId(getProductId1());
        txRestriction.setTimeFilter(TimeFilter.ever());
        return txRestriction;
    }

    private TransactionRestriction getHasEngagedPriorToFive() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedProd1() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        txRestriction.setTimeFilter(TimeFilter.ever());
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedProd2() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        txRestriction.setTimeFilter(TimeFilter.ever());
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotEngagedWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getEngagedWithinSeven() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(7));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getEngagedInCurrentPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedWithin() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(6));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedWithinOverMaxPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(40));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedPriorOnlyOverMaxPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(40));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountLessThan1M() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.LESS_THAN, Collections.singletonList(1000000.0), true);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountLessThan1MWithPurchase() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.LESS_THAN, Collections.singletonList(1000000.0), false);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalQuantityGTE10Once() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_OR_EQUAL, Collections.singletonList(10.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getSumAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        txRestriction.setTargetProductId(getProductId2());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Collections.singletonList(5000));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getPriorSevenEngaged() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                PeriodStrategy.Template.Month.name(), //
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
        frontEndQuery.setTargetProductIds(Arrays.asList(getProductId1()));
        return frontEndQuery;
    }

    public TransactionRestriction getSumQuantity() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        // 3872223C9BA06C649D68E415E23A9446
        txRestriction.setProductId(getProductId2());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.SUM,
                ComparisonType.LESS_THAN, Arrays.asList(20), true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Arrays.asList(3000));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceAmountBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Arrays.asList(1000));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAmountEachWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.EACH,
                ComparisonType.LESS_THAN, Arrays.asList(100), true);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getEachAmount() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.EACH,
                ComparisonType.GREATER_THAN, Arrays.asList(0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getEachAmountWithinFivePeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.EACH,
                ComparisonType.GREATER_THAN, Arrays.asList(0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAvgQuantity() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId1());
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AVG,
                ComparisonType.GREATER_OR_EQUAL, Arrays.asList(1.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAvgAmountInCurrentPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AVG,
                ComparisonType.GREATER_OR_EQUAL, Arrays.asList(100.0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getTotalAmountBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        txRestriction.setTargetProductId(getProductId1());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Arrays.asList(5000.0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAvgAmountWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AVG,
                ComparisonType.GREATER_THAN, Arrays.asList(1000.0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAvgAmountBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AVG,
                ComparisonType.LESS_OR_EQUAL, Arrays.asList(10), true);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getEachQuantityInCurrentPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.EACH,
                ComparisonType.GREATER_THAN, Arrays.asList(10.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAvgQuantityWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AVG,
                ComparisonType.LESS_OR_EQUAL, Arrays.asList(10.0), true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getTotalQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.SUM,
                ComparisonType.LESS_THAN, Arrays.asList(100.0), true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_THAN, Arrays.asList(50.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAtLeastOnceQuantityWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AT_LEAST_ONCE,
                ComparisonType.GREATER_OR_EQUAL, Arrays.asList(100.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getEachQuantityWithinPeriod() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.EACH,
                ComparisonType.GREATER_THAN, Arrays.asList(0.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    public TransactionRestriction getAvgQuantityBetweenPeriods() {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(getProductId2());
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AVG,
                ComparisonType.LESS_THAN, Arrays.asList(1.0), true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasPurchasedLater(TransactionRestriction original, int laggingPeriodCount) {
        TimeFilter timeFilter = new TimeFilter(original.getTimeFilter().getLhs(), ComparisonType.FOLLOWING,
                original.getTimeFilter().getPeriod(), Arrays.asList(laggingPeriodCount, laggingPeriodCount));

        String targetProductId = original.getTargetProductId() == null ? original.getProductId()
                : original.getTargetProductId();

        return new TransactionRestriction(targetProductId, //
                timeFilter, //
                false, //
                null, //
                null);
    }

    private TransactionRestriction getHasPurchasedLaterWithAmount(TransactionRestriction original,
            int laggingPeriodCount, int totalAmount) {
        TimeFilter timeFilter = new TimeFilter(original.getTimeFilter().getLhs(), ComparisonType.FOLLOWING,
                original.getTimeFilter().getPeriod(), Arrays.asList(laggingPeriodCount, laggingPeriodCount));

        String targetProductId = original.getTargetProductId() == null ? original.getProductId()
                : original.getTargetProductId();

        AggregationFilter spentFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Arrays.asList(totalAmount));

        return new TransactionRestriction(targetProductId, //
                timeFilter, //
                false, //
                spentFilter, //
                null);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalOrOneChild(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasEngaged();
        Restriction l = Restriction.builder().or(txRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalAndOneChild(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasEngaged();
        Restriction l = Restriction.builder().and(txRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalAndTwoChildren(String sqlUser, String queryContext) {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalWithTwoLevels(String sqlUser, String queryContext) {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        TransactionRestriction t3 = getHasEngaged();
        TransactionRestriction t4 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        Restriction l2 = Restriction.builder().and(t3, t4, l1).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l2, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalWithTwoFullLevels(String sqlUser, String queryContext) {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        TransactionRestriction t3 = getHasEngaged();
        TransactionRestriction t4 = getSumAmount();
        Restriction l1 = Restriction.builder().or(t1, t2).build();
        Restriction l2 = Restriction.builder().or(t3, t4).build();
        Restriction l3 = Restriction.builder().and(l1, l2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l3, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalOrTwoChildren(String sqlUser, String queryContext) {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().or(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasEngaged(String sqlUser, String queryContext) {
        // has engaged
        TransactionRestriction txRestriction = getHasEngaged();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 115);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasNotEngaged(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasNotEngagedProd1();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1216);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasEngagedForTraining(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasEngaged();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1857);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testGetDistinctCount(String sqlUser, String queryContext) {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Transaction, "ProductId").eq(PROD_ID1) //
                .build();
        Query query = Query.builder()
                .select(new AttributeLookup(BusinessEntity.Transaction, InterfaceName.AccountId.name())) //
                .distinct(true)
                .from(BusinessEntity.Transaction).where(restriction).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 115);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testFirstPurchaseForEvent(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasNotEngagedProd1();
        TransactionRestriction nextRestriction = getHasPurchasedLaterWithAmount(txRestriction, 1, 200);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 52);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasEngagedForEvent(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasEngaged();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 318);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasEngagedForEventWithRevenue(String sqlUser, String queryContext) throws SQLException {
        TransactionRestriction txRestriction = getHasEngaged();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                getEventFrontEndQueryWithProductRevenue(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        verifyHasEngaedForEventWithRevenue(query, sqlUser, 318);
    }

    protected long verifyHasEngaedForEventWithRevenue(Query query, String sqlUser, long expectedCount)
            throws SQLException {
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        ResultSet result = sqlQuery.getResults();
        ResultSetMetaData metaData = result.getMetaData();
        Assert.assertEquals(metaData.getColumnCount(), 3);
        int count = 0;
        while (result.next()) {
            Assert.assertTrue(result.getInt("revenue") > 0);
            count += 1;
        }
        Assert.assertEquals(count, expectedCount);
        return count;
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasEngagedPrior(String sqlUser, String queryContext) {
        // has engaged
        TransactionRestriction txRestriction = getHasEngagedPriorToFive();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 50);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasPurchasedInCurrentPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getEngagedInCurrentPeriod();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 18);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasNotPurchasedWithin(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasNotPurchasedWithin();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1265);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasNotPurchasedWithinOverMaxPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasNotPurchasedWithinOverMaxPeriod();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 0);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasNotPurchasedPriorOnlyOverMaxPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasNotPurchasedPriorOnlyOverMaxPeriod();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 0);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalAndTwoChildrenForTraining(String sqlUser, String queryContext) {
        TransactionRestriction t1 = getHasEngaged();
        TransactionRestriction t2 = getSumAmount();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(),
                Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 9);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testLogicalAndTwoChildrenForEvent(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getHasEngaged();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction t1 = Restriction.builder().and(txRestriction, nextRestriction).build();
        TransactionRestriction sumRestriction = getSumAmount();
        TransactionRestriction sumNextRestriction = getHasPurchasedLater(sumRestriction, 1);
        Restriction t2 = Restriction.builder().and(sumRestriction, sumNextRestriction).build();
        Restriction l1 = Restriction.builder().and(t1, t2).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator
                .translateForEvent(queryFactory, attrRepo, l1, getDefaultEventFrontEndQuery(), Query.builder(), sqlUser)
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountLessThan1M(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalAmountLessThan1M();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1331);

    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountLessThan1MWithPurchase(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalAmountLessThan1MWithPurchase();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 115);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testQuantityAtLeastOnce(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalQuantityGTE10Once();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 42);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalQuantity(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getSumQuantity();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAmountAtLeastOnce(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAtLeastOnceAmount();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAmountAtLeastOnceBetweenPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAtLeastOnceAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 7);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachAmountEver(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getEachAmount();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 0);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachAmountWithinFivePeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getEachAmountWithinFivePeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 2);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityEver(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgQuantity();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 16);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgAmountInCurrentPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgAmountInCurrentPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 15);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountBetweenPeriodsForTraining(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 21);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountBetweenPeriodsForEvent(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods();
        TransactionRestriction nextRestriction = getHasPurchasedLater(txRestriction, 1);
        Restriction eventRestriction = Restriction.builder().and(txRestriction, nextRestriction).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, eventRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 0);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountBetweenPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgAmountWithinPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgAmountWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 0);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAmountEachLessThanWithinPeriod(String sqlUser, String queryContext) {
        TransactionRestriction amount = getAmountEachWithinPeriod();
        TransactionRestriction hasNotEngaged = getHasNotEngagedWithinPeriod();
        Restriction logicalRestriction = Restriction.builder().or(amount, hasNotEngaged).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, logicalRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1277);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgAmountBetweenPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgAmountBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1180);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachQuantityInCurrentPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getEachQuantityInCurrentPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 5);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityWithinPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgQuantityWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1326);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalQuantityBetweenPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getTotalQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1290);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAtLeastOnceQuantityBetweenPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAtLeastOnceQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAtLeastOnceQuantityWithinPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAtLeastOnceQuantityWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 0);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachQuantityWithinPeriod(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getEachQuantityWithinPeriod();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 2);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityBetweenPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1219);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityBetweenPeriodsWithLimitedPeriods(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getAvgQuantityBetweenPeriods();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForTraining(queryFactory, attrRepo, txRestriction,
                getEventFrontEndQueryWithLimitedPeriodCount(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 11216);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testPriorOnly(String sqlUser, String queryContext) {
        TransactionRestriction txRestriction = getPriorSevenEngaged();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 42);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testPriorOnlyNegativeCase(String sqlUser, String queryContext) {
        EventQueryTranslator eventTranslator = getEventQueryTranslator();

        TransactionRestriction txRestriction = getPriorSevenEngaged();
        txRestriction.setNegate(true);
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1289);

        TransactionRestriction hasNotEngaged = getHasNotEngagedProd2();
        TransactionRestriction within = getEngagedWithinSeven();
        Restriction logicalRestriction = Restriction.builder().or(hasNotEngaged, within).build();
        Query query1 = eventTranslator.translateForScoring(queryFactory, attrRepo, logicalRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery1 = queryEvaluator.evaluate(attrRepo, query1, sqlUser);
        logQuery(sqlUser, sqlQuery1);
        testGetCountAndAssert(sqlUser, query1, 1289);

    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testPriorOnlyTwoLevel(String sqlUser, String queryContext) {
        TransactionRestriction priorOnlyRestriction = getPriorSevenEngaged();
        TransactionRestriction sumAmountRestriction = getSumAmount();
        Restriction priorAndSumRestriction = Restriction.builder().or(sumAmountRestriction, priorOnlyRestriction)
                .build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, priorAndSumRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 50);

    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testSimpleAccountQuery(String sqlUser, String queryContext) {
        Restriction acctRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .eq(getAccountId1()).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, acctRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        // SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query,
        // sqlUser);
        // logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1);

    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountEventQuery(String sqlUser, String queryContext) {
        Restriction acctRestriction = Restriction.builder().let(BusinessEntity.Account, ATTR_ACCOUNT_ID)
                .eq(getAccountId2()).build();
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForEvent(queryFactory, attrRepo, acctRestriction,
                getEventFrontEndQueryWithProductRevenue(), Query.builder(), sqlUser).build();
        /*
         * SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query,
         * sqlUser); logQuery(sqlUser, sqlQuery);
         */
        testGetCountAndAssert(sqlUser, query, 26);
    }

    public static class EnhancedEventQueryTranslator extends EventQueryTranslator {
        @Override
        protected String getPeriodTransactionTableName(AttributeRepository repository) {
            return "tftest_8_periodtransaction_2018_01_06_00_57_09_utc";
        }
    }
}
