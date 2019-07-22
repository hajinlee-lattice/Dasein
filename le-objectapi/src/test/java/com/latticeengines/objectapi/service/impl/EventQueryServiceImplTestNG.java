package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;

import java.util.Collections;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

public class EventQueryServiceImplTestNG extends QueryServiceImplTestNGBase {

    @Autowired
    private EventQueryService eventQueryService;

    private static final String PRODUCT_ID = "6368494B622E0CB60F9C80FEB1D0F95F";

    protected boolean isSparkSQLTest = false;

    @BeforeClass(groups = { "functional", SPARK_TEST_GROUP })
    public void setup() {
        super.setupTestData(1);
    }

    @DataProvider(name = "userContexts")
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SEGMENT_USER, "Redshift" }
        };
    }

    protected EventQueryService getEventQueryService(String sqlUser) {
        return eventQueryService;
    }

    protected String getProductId() {
        return PRODUCT_ID;
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testScoringCount(String sqlUser, String queryContext) {
        // Ever, Amount > 0 and Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(0));
        Bucket.Transaction txn = new Bucket.Transaction(getProductId(), TimeFilter.ever(), greaterThan0, greaterThan0,
                false);
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Restriction txnRestriction = new BucketRestriction(attrLookup, Bucket.txnBkt(txn));
        Restriction accRestriction1 = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.City.name()) //
                .eq("HOUSTON") //
                .build();
        Restriction accRestriction2 = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.City.name()) //
                .eq("CHICAGO") //
                .build();
        Restriction accRestriction = Restriction.builder().or(accRestriction1, accRestriction2).build();
        Restriction cntRestriction1 = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.Email.name()) //
                .contains("gmail") //
                .build();
        Restriction cntRestriction2 = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.Email.name()) //
                .contains("hotmail") //
                .build();
        Restriction cntRestriction = Restriction.builder().or(cntRestriction1, cntRestriction2).build();

        Restriction restriction;
        if (!isSparkSQLTest) {
            // only transaction restriction
            verifyScoringQuery(sqlUser, txnRestriction, 832L);

            // only account restriction
            verifyScoringQuery(sqlUser, accRestriction1, 9L);
            verifyScoringQuery(sqlUser, accRestriction, 17L);

            // only contact restriction
            verifyScoringQuery(sqlUser, cntRestriction1, 18L);
            verifyScoringQuery(sqlUser, cntRestriction, 23L);

            // account + transaction
            restriction = Restriction.builder().and(accRestriction, txnRestriction).build();
            verifyScoringQuery(sqlUser, restriction, 11L);
        }
        // account + contact + transaction
        restriction = Restriction.builder().and(accRestriction, cntRestriction, txnRestriction).build();
        verifyScoringQuery(sqlUser, restriction, 1L);
    }

    private void verifyScoringQuery(String sqlUser, Restriction restriction, long expectedCount) {
        long count = countRestrictionForScoring(sqlUser, restriction);
        testAndAssertCount(sqlUser, count, expectedCount);
        int numTuples = (int) Math.min(5L, count);
        if (numTuples > 0) {
            DataPage dataPage = retrieveScoringDataByRestriction(sqlUser, restriction, numTuples);
            Assert.assertTrue(CollectionUtils.isNotEmpty(dataPage.getData()));
            testAndAssertCount(sqlUser, dataPage.getData().size(), numTuples);
        }
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, expectedExceptions = QueryEvaluationException.class, dataProvider = "userContexts")
    public void testCrossPeriodQuery(String sqlUser, String queryContext) {
        Bucket.Transaction txn1 = new Bucket.Transaction(getProductId(), TimeFilter.ever(), null, null, false);
        TimeFilter timeFilter = TimeFilter.ever();
        timeFilter.setPeriod(PeriodStrategy.Template.Quarter.name());
        Bucket.Transaction txn2 = new Bucket.Transaction(getProductId(), timeFilter, null, null, false);

        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction1 = new BucketRestriction(attrLookup, Bucket.txnBkt(txn1));
        Restriction restriction2 = new BucketRestriction(attrLookup, Bucket.txnBkt(txn2));
        Restriction restriction = Restriction.builder().and(restriction1, restriction2).build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        getEventQueryService(sqlUser).getScoringTuples(frontEndQuery, DataCollection.Version.Blue);
    }

    @Test(groups = { "functional", SPARK_TEST_GROUP }, dataProvider = "userContexts")
    public void testHasEngaged(String sqlUser, String queryContext) {
        Bucket.Transaction txn = new Bucket.Transaction(getProductId(), TimeFilter.ever(), null, null, false);

        long scoringCount = countTxnBktForScoring(sqlUser, txn);
        testAndAssertCount(sqlUser, scoringCount, 832);

        if (!isSparkSQLTest) {
            scoringCount = countTxnBktForScoringUsingSegmentQuery(sqlUser, txn);
            testAndAssertCount(sqlUser, scoringCount, 832);
        }

        long trainingCount = countTxnBktForTraining(sqlUser, txn);
        testAndAssertCount(sqlUser, trainingCount, 16378);

        long eventCount = countTxnBktForEvent(sqlUser, txn);
        testAndAssertCount(sqlUser, eventCount, 5374);
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testDP6881(String sqlUser, String queryContext) {
        EventFrontEndQuery frontEndQuery = loadEventFrontEndQueryFromResource("dp6881.json");
        long count = getEventQueryService(sqlUser).getTrainingCount(frontEndQuery, DataCollection.Version.Blue);
        testAndAssertCount(sqlUser, count, 8501);
    }

    @Test(groups = "functional")
    public void testSparkSQLQuery() {
        EventFrontEndQuery frontEndQuery = loadEventFrontEndQueryFromResource("prior.json");
        frontEndQuery.getSegmentQuery().setEvaluationDateStr(maxTransactionDate);
        String sql = eventQueryService.getQueryStr(frontEndQuery, EventType.Scoring, //
                DataCollection.Version.Blue);
        System.out.println(sql);

//        frontEndQuery = loadEventFrontEndQueryFromResource("fisher2.json");
//        frontEndQuery.getSegmentQuery().setEvaluationDateStr(maxTransactionDate);
//        sql = eventQueryService.getQueryStr(frontEndQuery, EventType.Scoring, DataCollection.Version.Blue);
//        System.out.println(sql);
    }

    private long countTxnBktForScoring(String sqlUser, Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        return countRestrictionForScoring(sqlUser, restriction);
    }

    private long countTxnBktForScoringUsingSegmentQuery(String sqlUser, Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        return countRestrictionForScoringUsingSegmentQuery(sqlUser, restriction);
    }

    private long countRestrictionForScoring(String sqlUser, Restriction restriction) {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        frontEndQuery.setTargetProductIds(Collections.singletonList(getProductId()));
        frontEndQuery.setPeriodName("Month");
        System.out.println(eventQueryService.getQueryStr(frontEndQuery.getDeepCopy(), //
                EventType.Scoring, DataCollection.Version.Blue));
        return getEventQueryService(sqlUser).getScoringCount(frontEndQuery, DataCollection.Version.Blue);
    }

    private long countRestrictionForScoringUsingSegmentQuery(String sqlUser, Restriction restriction) {
        EventFrontEndQuery eventQuery = new EventFrontEndQuery();
        FrontEndQuery segmentQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        segmentQuery.setAccountRestriction(frontEndRestriction);
        segmentQuery.setMainEntity(BusinessEntity.Account);
        segmentQuery.setEvaluationDateStr(maxTransactionDate);
        eventQuery.setSegmentQuery(segmentQuery);
        eventQuery.setPageFilter(new PageFilter(0, 0));
        eventQuery.setTargetProductIds(Collections.singletonList(getProductId()));
        eventQuery.setPeriodName("Month");
        System.out.println(eventQueryService.getQueryStr(eventQuery.getDeepCopy(), //
                EventType.Scoring, DataCollection.Version.Blue));
        return getEventQueryService(sqlUser).getScoringCount(eventQuery, DataCollection.Version.Blue);
    }

    private DataPage retrieveScoringDataByRestriction(String sqlUser, Restriction restriction, int numTuples) {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, numTuples));
        frontEndQuery.setTargetProductIds(Collections.singletonList(getProductId()));
        frontEndQuery.setPeriodName("Month");
        return getEventQueryService(sqlUser).getScoringTuples(frontEndQuery, DataCollection.Version.Blue);
    }

    private long countTxnBktForTraining(String sqlUser, Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        frontEndQuery.setPeriodName("Month");
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        return getEventQueryService(sqlUser).getTrainingCount(frontEndQuery, DataCollection.Version.Blue);
    }

    private long countTxnBktForEvent(String sqlUser, Bucket.Transaction txn) {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();

        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        TimeFilter nextPeriod = TimeFilter.following(1, 1, txn.getTimeFilter().getPeriod());
        Bucket.Transaction next = new Bucket.Transaction(txn.getProductId(), nextPeriod, null, null, false);
        Bucket nextBucket = Bucket.txnBkt(next);
        Restriction nextRestriction = new BucketRestriction(attrLookup, nextBucket);
        Restriction eventRestriction = Restriction.builder().and(restriction, nextRestriction).build();

        frontEndRestriction.setRestriction(eventRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        frontEndQuery.setPeriodName("Month");
        return getEventQueryService(sqlUser).getEventCount(frontEndQuery, DataCollection.Version.Blue);
    }

}
