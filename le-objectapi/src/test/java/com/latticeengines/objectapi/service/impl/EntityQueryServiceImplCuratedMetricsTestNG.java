package com.latticeengines.objectapi.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.EntityQueryService;

public class EntityQueryServiceImplCuratedMetricsTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private EntityQueryService entityQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(3);
    }

    private EntityQueryService getEntityQueryService() {
        return entityQueryService;
    }

    @Test(groups = "functional")
    public void testChangeBucket() {
        String sqlUser = SEGMENT_USER;
        final String phAttr = "AM_uKt9Tnd4sTXNUxEMzvIXcC9eSkaGah8__M_1_6__AS";
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.chgBkt(Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                Collections.singletonList(5));
        Restriction restriction = new BucketRestriction(BusinessEntity.PurchaseHistory, phAttr, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.PurchaseHistory, phAttr);
        long count = getEntityQueryService().getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        Assert.assertEquals(count, 33248);
        testAndAssertCount(sqlUser, count, 33248);
    }

}
