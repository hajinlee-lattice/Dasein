package com.latticeengines.query.evaluator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
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

public class EventQueryTranslatorMultiProductTestNG extends QueryFunctionalTestNGBase {

    private EventQueryTranslator getEventQueryTranslator() {
        return new EnhancedEventQueryTranslator();
    }

    private List<String> productIds = Arrays.asList(
            "3872223C9BA06C649D68E415E23A9446", // Product 0
            "6B047FF03A59E42A79D1961541C1BF60", // Product 1
            "FE4D73162629DF254688CC8D553FA52A", // Product 2
            "3CA17E482511F94FCEB93195D2B146DE", // Product 3
            "94842E58561D516577E4C0377F601DA3", // Product 4
            "8D6A468D2B5F99D5D2252DE0DF6F971C", // Product 5
            "A78DF03BAC196BE9A08508FFDB433A31", // Product 6
            "40BE92E2D8ADE18DC80A3FAEC761F91A", // Product 7
            "D7B8185BB08C1AFA624B7B9BA49AA77C", // Product 8
            "6C5E5F2F07EB909FDB480F868047586C", // Product 9
            "64E8729DCA85D713B8BE424E6A2828E5", // Product 10
            "EBB5D5D47420FE96255F586D37FE3EB0", // Product 11
            "56E3D7F5C674E7AC8026A61360A80769", // Product 12
            "B365E24EC8F2C3672684ACC2F2EE208A", // Product 13
            "D45763A3E784654F2C17CF906F9CD295", // Product 14
            "7D18DD009AC1004A4E72B66E2D8F594F", // Product 15
            "5544C27D9C475BF2B639241F65D8D26F", // Product 16
            "E49000DA92C149E7BF960306185BF577", // Product 17
            "36E2AF3FC4AE03328EE407C3A3933007", // Product 18
            "1EF99C5568D3047FBD9D4A271614B008", // Product 19
            "CFF528B14A522C97F5F23964583EDB78", // Product 20
            "2A2A5856EC1CCB78E786DF65564DA39E" // Product 21
    );

    private List<String> getProductIds() {
        return productIds;
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SQL_USER, "Redshift" }
        };
    }

    private int getDefaultPeriodId() {
        return 10;
    }

    private TransactionRestriction getHasEngagedPriorToFive(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                PeriodStrategy.Template.Month.name(), //
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

    private TransactionRestriction getHasNotEngagedWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getEngagedInCurrentPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private TransactionRestriction getHasNotPurchasedWithin(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(6));
        txRestriction.setTimeFilter(timeFilter);
        txRestriction.setNegate(true);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountLessThan1M(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.SUM, ComparisonType.LESS_THAN, Collections.singletonList(1000000.0),
                true);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalQuantityGTE10Once(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.AT_LEAST_ONCE, ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(10.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getPriorSevenEngaged(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.PRIOR_ONLY, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(7));
        txRestriction.setTimeFilter(timeFilter);
        return txRestriction;
    }

    private EventFrontEndQuery getDefaultEventFrontEndQuery() {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setPeriodName("Month");
        frontEndQuery.setEvaluationPeriodId(getDefaultPeriodId());
        return frontEndQuery;
    }

    private TransactionRestriction getAtLeastOnceAmountBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.AT_LEAST_ONCE, ComparisonType.GREATER_THAN,
                Collections.singletonList(1000));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAmountEachWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.EACH, ComparisonType.LESS_THAN, Collections.singletonList(100),
                true);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachAmount(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.EACH, ComparisonType.GREATER_THAN, Collections.singletonList(0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachAmountWithinFivePeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.EACH, ComparisonType.GREATER_THAN, Collections.singletonList(0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgQuantity(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        txRestriction.setTimeFilter(TimeFilter.ever());
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.AVG, ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(1.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgAmountInCurrentPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.AVG, ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(100.0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalAmountBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.SUM, ComparisonType.GREATER_THAN,
                Collections.singletonList(5000.0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgAmountWithinPeriod(String prodIdList) {
        // XXX
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.AVG, ComparisonType.GREATER_THAN,
                Collections.singletonList(1000.0));
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgAmountBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.SPENT,
                AggregationType.AVG, ComparisonType.LESS_OR_EQUAL, Collections.singletonList(10),
                true);
        txRestriction.setSpentFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachQuantityInCurrentPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(0));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.EACH, ComparisonType.GREATER_THAN, Collections.singletonList(10.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgQuantityWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.AVG, ComparisonType.LESS_OR_EQUAL, Collections.singletonList(10.0),
                true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getTotalQuantityBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.SUM, ComparisonType.LESS_THAN, Collections.singletonList(100.0),
                true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAtLeastOnceQuantityBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(5, 10));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.AT_LEAST_ONCE, ComparisonType.GREATER_THAN,
                Collections.singletonList(50.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAtLeastOnceQuantityWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.AT_LEAST_ONCE, ComparisonType.GREATER_OR_EQUAL,
                Collections.singletonList(100.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getEachQuantityWithinPeriod(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.WITHIN, //
                PeriodStrategy.Template.Month.name(), //
                Collections.singletonList(5));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.EACH, ComparisonType.GREATER_THAN, Collections.singletonList(0.0));
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    private TransactionRestriction getAvgQuantityBetweenPeriods(String prodIdList) {
        TransactionRestriction txRestriction = new TransactionRestriction();
        txRestriction.setProductId(prodIdList);
        TimeFilter timeFilter = new TimeFilter(ComparisonType.BETWEEN, //
                PeriodStrategy.Template.Month.name(), //
                Arrays.asList(7, 30));
        txRestriction.setTimeFilter(timeFilter);
        AggregationFilter aggFilter = new AggregationFilter(AggregationSelector.UNIT,
                AggregationType.AVG, ComparisonType.LESS_THAN, Collections.singletonList(1.0),
                true);
        txRestriction.setUnitFilter(aggFilter);
        return txRestriction;
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasEngagedPrior(String sqlUser, String queryContext) {
        // DS_Test_1
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(1), getProductIds().get(2));
        TransactionRestriction txRestriction = getHasEngagedPriorToFive(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 5966, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasPurchasedInCurrentPeriod(String sqlUser, String queryContext) {
        // DS_Test_2
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(1), getProductIds().get(2));
        TransactionRestriction txRestriction = getEngagedInCurrentPeriod(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 2862, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasNotPurchasedWithin(String sqlUser, String queryContext) {
        // DS_Test_3
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(3), getProductIds().get(4));
        TransactionRestriction txRestriction = getHasNotPurchasedWithin(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 76987, false);
    }

    // @Test(groups = "functional", dataProvider = "userContexts")
    public void testHasNotEngaged(String sqlUser, String queryContext) {
        // DS_Test_4
        String prodIdList = "tbd";
        TransactionRestriction txRestriction = getHasNotEngagedProd1(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 68680, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testPriorOnlyNegativeCase(String sqlUser, String queryContext) {
        // DS_Test_5
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(5), getProductIds().get(6));
        EventQueryTranslator eventTranslator = getEventQueryTranslator();

        TransactionRestriction txRestriction = getPriorSevenEngaged(prodIdList);
        txRestriction.setNegate(true);
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 88959, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountLessThan1M(String sqlUser, String queryContext) {
        // DS_Test_6
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(7), getProductIds().get(8), getProductIds().get(9));
        TransactionRestriction txRestriction = getTotalAmountLessThan1M(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 96058, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachAmountEver(String sqlUser, String queryContext) {
        // DS_Test_7
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(10), getProductIds().get(11));
        TransactionRestriction txRestriction = getEachAmount(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 210, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityEver(String sqlUser, String queryContext) {
        // DS_Test_8
        String prodIdList = String.join(",", getProductIds().get(0), getProductIds().get(12));
        TransactionRestriction txRestriction = getAvgQuantity(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 13985, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testQuantityAtLeastOnce(String sqlUser, String queryContext) {
        // DS_Test_9
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(13));
        TransactionRestriction txRestriction = getTotalQuantityGTE10Once(prodIdList);
        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 1196, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgAmountInCurrentPeriod(String sqlUser, String queryContext) {
        // DS_Test_10
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(5),getProductIds().get(14));
        TransactionRestriction txRestriction = getAvgAmountInCurrentPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 3795, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachQuantityWithinPeriod(String sqlUser, String queryContext) {
        // DS_Test_11
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(15),getProductIds().get(16));
        TransactionRestriction txRestriction = getEachQuantityWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 318, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAmountAtLeastOnceBetweenPeriods(String sqlUser, String queryContext) {
        // DS_Test_12
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(17),getProductIds().get(18));
        TransactionRestriction txRestriction = getAtLeastOnceAmountBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 6023, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalAmountBetweenPeriods(String sqlUser, String queryContext) {
        // DS_Test_13
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(19),getProductIds().get(20));
        TransactionRestriction txRestriction = getTotalAmountBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 4464, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgAmountWithinPeriods(String sqlUser, String queryContext) {
        // DS_Test_14
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(20));
        TransactionRestriction txRestriction = getAvgAmountWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 2881, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAmountEachLessThanWithinPeriod(String sqlUser, String queryContext) {
        // DS_Test_15
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(20));
        TransactionRestriction amount = getAmountEachWithinPeriod(prodIdList);
        TransactionRestriction hasNotEngaged = getHasNotEngagedWithinPeriod(prodIdList);
        Restriction logicalRestriction = Restriction.builder().or(amount, hasNotEngaged).build();

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo,
                logicalRestriction, getDefaultEventFrontEndQuery(), Query.builder(), sqlUser)
                .build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 85989, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgAmountBetweenPeriods(String sqlUser, String queryContext) {
        // DS_Test_16
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(4),getProductIds().get(20));
        TransactionRestriction txRestriction = getAvgAmountBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 82723, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachQuantityInCurrentPeriod(String sqlUser, String queryContext) {
        // DS_Test_17
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(4),getProductIds().get(20));
        TransactionRestriction txRestriction = getEachQuantityInCurrentPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 776, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityWithinPeriod(String sqlUser, String queryContext) {
        // DS_Test_18
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(20));
        TransactionRestriction txRestriction = getAvgQuantityWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 95895, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testTotalQuantityBetweenPeriods(String sqlUser, String queryContext) {
        // DS_Test_19
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(20));
        TransactionRestriction txRestriction = getTotalQuantityBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 95949, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAtLeastOnceQuantityBetweenPeriods(String sqlUser, String queryContext) {
        // DS_Test_20
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(20));
        TransactionRestriction txRestriction = getAtLeastOnceQuantityBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 97, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAtLeastOnceQuantityWithinPeriod(String sqlUser, String queryContext) {
        // DS_Test_21
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(4),getProductIds().get(6));
        TransactionRestriction txRestriction = getAtLeastOnceQuantityWithinPeriod(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 596, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testEachAmountWithinFivePeriods(String sqlUser, String queryContext) {
        // DS_Test_22
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(20));
        TransactionRestriction txRestriction = getEachAmountWithinFivePeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 317, false);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAvgQuantityBetweenPeriods(String sqlUser, String queryContext) {
        // DS_Test_23
        String prodIdList = String.join(",", getProductIds().get(0),getProductIds().get(21),getProductIds().get(4));
        TransactionRestriction txRestriction = getAvgQuantityBetweenPeriods(prodIdList);

        EventQueryTranslator eventTranslator = getEventQueryTranslator();
        Query query = eventTranslator.translateForScoring(queryFactory, attrRepo, txRestriction,
                getDefaultEventFrontEndQuery(), Query.builder(), sqlUser).build();
        SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
        logQuery(sqlUser, sqlQuery);
        testGetCountAndAssert(sqlUser, query, 89763, false);
    }

    public static class EnhancedEventQueryTranslator extends EventQueryTranslator {
        @Override
        protected String getPeriodTransactionTableName(AttributeRepository repository) {
            return "tftest_8_periodtransaction_2018_01_06_00_57_09_utc";
        }

    }

}
