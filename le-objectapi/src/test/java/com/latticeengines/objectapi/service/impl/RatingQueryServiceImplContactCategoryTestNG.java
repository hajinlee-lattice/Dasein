package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.AccountMarketingActivity;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.objectapi.service.RatingQueryService;

public class RatingQueryServiceImplContactCategoryTestNG extends QueryServiceImplTestNGBase {

    private static final String ATTR_ACCOUNT_NAME = "CompanyName";
    private static final String ATTR_ACC_MA = "am_mbaaa2__9__w_8_w";
    private static final String ATTR_CON_MA = "am_mbaac2__9__w_8_w";

    @Inject
    private RatingQueryService ratingQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(6);
    }

    @Test(groups = "functional")
    public void testScoreData() {
        FrontEndQuery frontEndQuery = getSegmentQuery();
        RatingModel model = ruleBasedModel();
        frontEndQuery.setRatingModels(Collections.singletonList(model));

        DataPage dataPage = ratingQueryService.getData(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(dataPage);
        List<Map<String, Object>> data = dataPage.getData();
        Assert.assertFalse(data.isEmpty());
        data.forEach(row -> {
            Assert.assertTrue(row.containsKey("Score"), JsonUtils.serialize(row));
            String score = (String) row.get("Score");
            Assert.assertNotNull(score);
            Assert.assertTrue(
                    Arrays.asList(RatingBucketName.A.getName(), RatingBucketName.C.getName(), RatingBucketName.F.getName()).contains(score));
        });

        Map<String, Long> ratingCounts = ratingQueryService.getRatingCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertNotNull(ratingCounts);
        Assert.assertEquals(ratingCounts.get("A"), Long.valueOf(1));
        Assert.assertEquals(ratingCounts.get("C"), Long.valueOf(1));
        Assert.assertEquals(ratingCounts.get("F"), Long.valueOf(3));
    }


    private FrontEndQuery getSegmentQuery() {
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
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setSort(new FrontEndSort(Collections.singletonList(
                new AttributeLookup(BusinessEntity.Account, ATTR_ACCOUNT_NAME)), false));
        return frontEndQuery;
    }

    private RuleBasedModel ruleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        for (RatingBucketName bucket: RatingBucketName.values()) {
            rule.getBucketToRuleMap().put(bucket.getName(), defaultRulesFromUI());
        }

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.AccountMarketingActivity, ATTR_ACC_MA, Bucket.rangeBkt(90, null)));
        rule.getBucketToRuleMap().put(RatingBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                new BucketRestriction(BusinessEntity.AccountMarketingActivity, ATTR_ACC_MA, Bucket.rangeBkt(20, 90)));
        ruleC.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.ContactMarketingActivity, ATTR_CON_MA, Bucket.rangeBkt(10, null)));
        rule.getBucketToRuleMap().put(RatingBucketName.C.getName(), ruleC);

        rule.setDefaultBucketName(RatingBucketName.F.getName());

        model.setRatingRule(rule);

        return model;
    }

    private RuleBasedModel ruleBasedModelWithOnlyContactCategory() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        for (RatingBucketName bucket: RatingBucketName.values()) {
            rule.getBucketToRuleMap().put(bucket.getName(), defaultRulesFromUI());
        }

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                new BucketRestriction(BusinessEntity.ContactMarketingActivity, ATTR_CON_MA, Bucket.rangeBkt(10, null)));
        rule.getBucketToRuleMap().put(RatingBucketName.C.getName(), ruleC);

        rule.setDefaultBucketName(RatingBucketName.A.getName());

        model.setRatingRule(rule);

        return model;
    }

    private static Map<String, Restriction> defaultRulesFromUI() {
        Map<String, Restriction> map = new HashMap<>();
        map.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, LogicalRestriction.builder().and(Collections.emptyList()).build());
        map.put(FrontEndQueryConstants.CONTACT_RESTRICTION, LogicalRestriction.builder().and(Collections.emptyList()).build());
        return map;
    }

}
