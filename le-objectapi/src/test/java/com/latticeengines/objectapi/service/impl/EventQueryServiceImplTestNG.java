package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import java.util.Collections;

import org.apache.commons.collections4.CollectionUtils;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpException;
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
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class EventQueryServiceImplTestNG extends ObjectApiFunctionalTestNGBase {

    @Autowired
    private EventQueryService eventQueryService;

    @Autowired
    private QueryEvaluatorService queryEvaluatorService;

    private static final String PRODUCT_ID = "6368494B622E0CB60F9C80FEB1D0F95F";

    @BeforeClass(groups = "functional")
    public void setup() {
        mockDataCollectionProxy();
        MultiTenantContext.setTenant(new Tenant("LocalTest"));
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

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testCrossPeriodQuery() {
        Bucket.Transaction txn1 = new Bucket.Transaction(PRODUCT_ID, TimeFilter.ever(), null, null, false);
        TimeFilter timeFilter = TimeFilter.ever();
        timeFilter.setPeriod(TimeFilter.Period.Quarter.name());
        Bucket.Transaction txn2 = new Bucket.Transaction(PRODUCT_ID, timeFilter, null, null, false);

        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction1 = new BucketRestriction(attrLookup, Bucket.txnBkt(txn1));
        Restriction restriction2 = new BucketRestriction(attrLookup, Bucket.txnBkt(txn2));
        Restriction restriction = Restriction.builder().and(restriction1, restriction2).build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        eventQueryService.getScoringTuples(frontEndQuery);
    }

    @Test(groups = "functional")
    public void testHasEngaged() {
        Bucket.Transaction txn = new Bucket.Transaction(PRODUCT_ID, TimeFilter.ever(), null, null, false);
        long scoringCount = countTxnBktForScoring(txn);
        Assert.assertEquals(scoringCount, 832);
        long trainingCount = countTxnBktForTraining(txn);
        Assert.assertEquals(trainingCount, 16378);
        long eventCount = countTxnBktForEvent(txn);
        Assert.assertEquals(eventCount, 5374);
    }

    private long countTxnBktForScoringFromDataPage(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        DataPage dataPage = eventQueryService.getScoringTuples(frontEndQuery);
        Assert.assertNotNull(dataPage.getData());
        return dataPage.getData().size();
    }

    private long countTxnBktForScoring(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        return countRestrictionForScoring(restriction);
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
        return eventQueryService.getScoringCount(frontEndQuery);
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
        return eventQueryService.getScoringTuples(frontEndQuery);
    }

    private long countTxnBktForTraining(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        return eventQueryService.getTrainingCount(frontEndQuery);
    }

    private long countTxnBktForEvent(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        return eventQueryService.getEventCount(frontEndQuery);
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any())).thenReturn(attrRepo);
        queryEvaluatorService.setDataCollectionProxy(proxy);
    }
}
