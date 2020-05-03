package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.AccountMarketingActivity;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.EntityQueryService;

public class EntityQueryServiceImplContactCategoryTestNG extends QueryServiceImplTestNGBase {

    private static final String ATTR_ACC_MA = "am_mbaaa2__9__w_8_w";
    private static final String ATTR_CON_MA = "am_mbaac2__9__w_8_w";

    @Inject
    private EntityQueryService entityQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(6);
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] { { SEGMENT_USER, "Redshift" } };
    }

    private EntityQueryService getEntityQueryService(String sqlUser) {
        return entityQueryService;
    }

    @Test(groups = "functional", dataProvider = "userContexts")
    public void testAccountCount(String sqlUser, String queryContext) {
        FrontEndQuery frontEndQuery = getSegmentQuery();
        long count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        testAndAssertCount(sqlUser, count, 5);

        frontEndQuery.setMainEntity(BusinessEntity.Contact);
        count = getEntityQueryService(sqlUser).getCount(frontEndQuery, DataCollection.Version.Blue, sqlUser);
        testAndAssertCount(sqlUser, count, 32);
    }

    protected FrontEndQuery getSegmentQuery() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);

        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction bkt = new BucketRestriction(AccountMarketingActivity, ATTR_ACC_MA, Bucket.notNullBkt());
        Restriction accountRestriction = Restriction.builder().and(bkt).build();
        frontEndRestriction.setRestriction(accountRestriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);

        Restriction contactRestriction = //
                new BucketRestriction(BusinessEntity.ContactMarketingActivity, ATTR_CON_MA, Bucket.notNullBkt());
        frontEndQuery.setContactRestriction(new FrontEndRestriction(contactRestriction));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return frontEndQuery;
    }

}
