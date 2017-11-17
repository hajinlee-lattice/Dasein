package com.latticeengines.objectapi.service.impl;

import java.util.Collections;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
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
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import static org.mockito.ArgumentMatchers.any;

public class EventQueryServiceImplTestNG extends ObjectApiFunctionalTestNGBase {

    @Autowired
    private EventQueryService eventQueryService;

    @Autowired
    private QueryEvaluatorService queryEvaluatorService;

    @BeforeClass(groups = "functional")
    public void setup() {
        mockDataCollectionProxy();
        MultiTenantContext.setTenant(new Tenant("LocalTest"));
    }

    @Test(groups = "functional", enabled = false)
    public void testScoring() {
        String prodId = "A78DF03BAC196BE9A08508FFDB433A31";

        // Ever Purchased
        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), null, null, false);
        long totalCount = countTxnBktForScoring(txn);
        Assert.assertEquals(totalCount, 21199L);

        // Ever, Amount > 0 and Quantity > 0
        AggregationFilter greaterThan0 = new AggregationFilter(ComparisonType.GREATER_THAN, Collections.singletonList(0));
        txn = new Bucket.Transaction(prodId, TimeFilter.ever(), greaterThan0, greaterThan0, false);
        long count = countTxnBktForScoring(txn);
        Assert.assertEquals(count, 21197L);
    }

    private long countTxnBktForScoring(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));

        FrontEndRestriction contactFERestriction = new FrontEndRestriction();
        Restriction cntRestriction = Restriction.builder().let(BusinessEntity.Contact,
                                                               InterfaceName.Title.name()).eq("Buyer").build();
        contactFERestriction.setRestriction(cntRestriction);
        frontEndQuery.setContactRestriction(contactFERestriction);

        DataPage dataPage = eventQueryService.getScoringTuples(frontEndQuery);
        Assert.assertNotNull(dataPage.getData());
        return dataPage.getData().size();
    }

    private long countTxnBktForTraining(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        DataPage dataPage = eventQueryService.getTrainingTuples(frontEndQuery);
        Assert.assertNotNull(dataPage.getData());
        return dataPage.getData().size();
    }

    private long countTxnBktForEvent(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyThing");
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(new PageFilter(0, 0));
        DataPage dataPage = eventQueryService.getEventTuples(frontEndQuery);
        Assert.assertNotNull(dataPage.getData());
        return dataPage.getData().size();
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any())).thenReturn(attrRepo);
        queryEvaluatorService.setDataCollectionProxy(proxy);
    }
}
