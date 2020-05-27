package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;

import java.util.Collections;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
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

    protected EntityQueryService getEntityQueryService() {
        return entityQueryService;
    }

    protected String getSqlUser() {
        return SEGMENT_USER;
    }

    @Test(groups = {"functional", SPARK_TEST_GROUP})
    public void testChangeBucket() {
        final String phAttr = "AM_uKt9Tnd4sTXNUxEMzvIXcC9eSkaGah8__M_1_6__AS";
        Bucket bucket = Bucket.chgBkt(Bucket.Change.Direction.DEC, Bucket.Change.ComparisonType.AS_MUCH_AS,
                Collections.singletonList(5));
        long count = countBkt(phAttr, bucket);
        Assert.assertEquals(count, 33248);
    }

    // PLS-17293
    @Test(groups = {"functional", SPARK_TEST_GROUP})
    public void testGteOperator() {
        final String phAttr = "AM_uKt9Tnd4sTXNUxEMzvIXcC9eSkaGah8__M_7_12__AS";
        Bucket bkt1 = Bucket.rangeBkt(0, null, false, false); // > 0
        long count1 = countBkt(phAttr, bkt1);
        Assert.assertTrue(count1 > 0, String.valueOf(count1));
        Bucket bkt2 = Bucket.valueBkt(ComparisonType.IN_COLLECTION, Collections.singletonList(0)); // == 0
        long count2 = countBkt(phAttr, bkt2);
        Assert.assertTrue(count2 > 0, String.valueOf(count2));
        Bucket bkt3 = Bucket.rangeBkt(0, null, true, false); // >= 0
        long count3 = countBkt(phAttr, bkt3);
        Assert.assertEquals(count3, count1 + count2);

        bkt2 = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList(0)); // == 0
        count2 = countBkt(phAttr, bkt2);
        Assert.assertTrue(count2 > 0, String.valueOf(count2));


        bkt1 = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList(1.92)); // == 1.92
        count1 = countBkt(phAttr, bkt1);
        Assert.assertTrue(count1 > 0, String.valueOf(count1));
        bkt2 = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, Collections.singletonList(1.92)); // != 1.92
        count2 = countBkt(phAttr, bkt2);
        Assert.assertTrue(count2 > 0, String.valueOf(count2));
        bkt3 = Bucket.rangeBkt(0, null, true, false); // >= 0 (all)
        count3 = countBkt(phAttr, bkt3);
        Assert.assertEquals(count3, count1 + count2);
    }

    private long countBkt(String phAttr, Bucket bkt) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = new BucketRestriction(BusinessEntity.PurchaseHistory, phAttr, bkt);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.PurchaseHistory, phAttr);
        return getEntityQueryService().getCount(frontEndQuery, DataCollection.Version.Blue, getSqlUser());
    }

}
