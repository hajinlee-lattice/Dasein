package com.latticeengines.objectapi.service.impl;

import java.util.Collections;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
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

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup("1");
    }

    @Test(groups = "functional")
    public void testScoringCount() {
        // Ever, Amount > 0 and Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN, Collections.singletonList(0));
        Bucket.Transaction txn = new Bucket.Transaction(PRODUCT_ID, TimeFilter.ever(), greaterThan0, greaterThan0, false);
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
                .let(BusinessEntity.Contact, InterfaceName.Title.name()) //
                .contains("Vice") //
                .build();
        Restriction cntRestriction2 = Restriction.builder() //
                .let(BusinessEntity.Contact, InterfaceName.Title.name()) //
                .contains("Professor") //
                .build();
        Restriction cntRestriction = Restriction.builder().or(cntRestriction1, cntRestriction2).build();

        // only transaction restriction
        verifyScoringQuery(txnRestriction, 832L);

        // only account restriction
        verifyScoringQuery(accRestriction1, 9L);
        verifyScoringQuery(accRestriction, 17L);

        // only contact restriction
        verifyScoringQuery(cntRestriction1, 23L);
        verifyScoringQuery(cntRestriction, 73L);

        // account + transaction
        Restriction restriction = Restriction.builder().and(accRestriction, txnRestriction).build();
        verifyScoringQuery(restriction, 11L);

        // account + contact + transaction
        restriction = Restriction.builder().and(accRestriction, cntRestriction, txnRestriction).build();
        verifyScoringQuery(restriction, 1L);
    }

    private void verifyScoringQuery(Restriction restriction, long expectedCount) {
        long count = countRestrictionForScoring(restriction);
        Assert.assertEquals(count, expectedCount);
        int numTuples = (int) Math.min(5L, expectedCount);
        if (numTuples > 0) {
            DataPage dataPage = retrieveScoringDataByRestriction(restriction, numTuples);
            Assert.assertTrue(CollectionUtils.isNotEmpty(dataPage.getData()));
            Assert.assertEquals(dataPage.getData().size(), numTuples);
        }
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testCrossPeriodQuery() {
        Bucket.Transaction txn1 = new Bucket.Transaction(PRODUCT_ID, TimeFilter.ever(), null, null, false);
        TimeFilter timeFilter = TimeFilter.ever();
        timeFilter.setPeriod(PeriodStrategy.Template.Quarter.name());
        Bucket.Transaction txn2 = new Bucket.Transaction(PRODUCT_ID, timeFilter, null, null, false);

        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction1 = new BucketRestriction(attrLookup, Bucket.txnBkt(txn1));
        Restriction restriction2 = new BucketRestriction(attrLookup, Bucket.txnBkt(txn2));
        Restriction restriction = Restriction.builder().and(restriction1, restriction2).build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        eventQueryService.getScoringTuples(frontEndQuery, DataCollection.Version.Blue);
    }

    @Test(groups = "functional")
    public void testHasEngaged() {
        Bucket.Transaction txn = new Bucket.Transaction(PRODUCT_ID, TimeFilter.ever(), null, null, false);
        long scoringCount = countTxnBktForScoring(txn);
        Assert.assertEquals(scoringCount, 832);
        scoringCount = countTxnBktForScoringUsingSegmentQuery(txn);
        Assert.assertEquals(scoringCount, 832);
        long trainingCount = countTxnBktForTraining(txn);
        Assert.assertEquals(trainingCount, 16378);
        long eventCount = countTxnBktForEvent(txn);
        Assert.assertEquals(eventCount, 5374);
    }

    @Test(groups = "functional")
    public void testDP6881() {
        EventFrontEndQuery frontEndQuery = loadFrontEndQueryFromResource("/dp6881.json");
        long count = eventQueryService.getTrainingCount(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertEquals(count, 8501);
    }

    @SuppressWarnings("unused")
    private long countTxnBktForScoringFromDataPage(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        DataPage dataPage = eventQueryService.getScoringTuples(frontEndQuery, DataCollection.Version.Blue);
        Assert.assertNotNull(dataPage.getData());
        return dataPage.getData().size();
    }

    private long countTxnBktForScoring(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        return countRestrictionForScoring(restriction);
    }

    private long countTxnBktForScoringUsingSegmentQuery(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        return countRestrictionForScoringUsingSegmentQuery(restriction);
    }

    private long countRestrictionForScoring(Restriction restriction) {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        frontEndQuery.setTargetProductIds(Collections.singletonList(PRODUCT_ID));
        frontEndQuery.setPeriodName("Month");
        return eventQueryService.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
    }

    private long countRestrictionForScoringUsingSegmentQuery(Restriction restriction) {
        EventFrontEndQuery eventQuery = new EventFrontEndQuery();
        FrontEndQuery segmentQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        segmentQuery.setAccountRestriction(frontEndRestriction);
        segmentQuery.setMainEntity(BusinessEntity.Account);
        segmentQuery.setEvaluationDateStr(maxTransactionDate);
        eventQuery.setSegmentQuery(segmentQuery);
        eventQuery.setPageFilter(new PageFilter(0, 0));
        eventQuery.setTargetProductIds(Collections.singletonList(PRODUCT_ID));
        eventQuery.setPeriodName("Month");
        return eventQueryService.getScoringCount(eventQuery, DataCollection.Version.Blue);
    }

    private DataPage retrieveScoringDataByRestriction(Restriction restriction, int numTuples) {
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, numTuples));
        frontEndQuery.setTargetProductIds(Collections.singletonList(PRODUCT_ID));
        frontEndQuery.setPeriodName("Month");
        return eventQueryService.getScoringTuples(frontEndQuery, DataCollection.Version.Blue);
    }

    private long countTxnBktForTraining(Bucket.Transaction txn) {
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
        return eventQueryService.getTrainingCount(frontEndQuery, DataCollection.Version.Blue);
    }

    private long countTxnBktForEvent(Bucket.Transaction txn) {
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
        return eventQueryService.getEventCount(frontEndQuery, DataCollection.Version.Blue);
    }

}
