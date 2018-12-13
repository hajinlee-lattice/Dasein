package com.latticeengines.objectapi.service.impl;

import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public class TransactionServiceImplUnitTestNG {

    @Test(groups = "unit")
    public void testNeedTimeFilterTranslator() {
        FrontEndQuery fq = new FrontEndQuery();
        TransactionServiceImpl ts = new TransactionServiceImpl();
        TimeFilter last = new TimeFilter(ComparisonType.LAST, Collections.singletonList(7));

        Bucket bkt = Bucket.dateBkt(last);
        BucketRestriction bucketRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                bkt);
        fq.setAccountRestriction(new FrontEndRestriction(bucketRestriction));
        Assert.assertTrue(ts.needTimeFilterTranslator(fq));

        bkt.setDateFilter(null);
        bucketRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"), bkt);
        fq.setAccountRestriction(new FrontEndRestriction(bucketRestriction));
        Assert.assertFalse(ts.needTimeFilterTranslator(fq));
    }

}
