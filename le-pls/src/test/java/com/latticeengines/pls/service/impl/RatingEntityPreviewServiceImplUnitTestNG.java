package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public class RatingEntityPreviewServiceImplUnitTestNG {

    private RatingEntityPreviewServiceImpl ratingEntityPreviewService;

    private FrontEndQuery frontEndQuery;

    private RatingModel ratingModel;

    private List<String> selectedBuckets;

    @BeforeClass(groups = "unit")
    public void setup() {
        ratingEntityPreviewService = new RatingEntityPreviewServiceImpl();
        ratingModel = creaateRuleBasedModel();
        frontEndQuery = createFrontEndQuery();
        selectedBuckets = createSelectedBuckets();
    }

    @Test(groups = "unit")
    public void testSetSelectedBuckets() {
        ratingEntityPreviewService.setSelectedBuckets(frontEndQuery, selectedBuckets, ratingModel);
        String restrictionStr = frontEndQuery.getAccountRestriction().getRestriction().toString();
        Assert.assertTrue(restrictionStr.contains(RuleBucketName.A.getName()) && //
                restrictionStr.contains(RuleBucketName.A_MINUS.getName()) && //
                restrictionStr.contains(RuleBucketName.C.getName()));
    }

    private FrontEndQuery createFrontEndQuery() {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "CompanyName").gte("D").build();
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));
        frontEndQuery.setPageFilter(new PageFilter(0, 10));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return frontEndQuery;
    }

    private RuleBasedModel creaateRuleBasedModel() {
        RuleBasedModel model = new RuleBasedModel();
        model.setId(UuidUtils.shortenUuid(UUID.randomUUID()));
        RatingRule rule = RatingRule.constructDefaultRule();

        Map<String, Restriction> ruleA = new HashMap<>();
        ruleA.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Account, "CompanyName").in("B", "G").build());
        ruleA.put(FrontEndQueryConstants.CONTACT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Contact, "Title").in("A", "N").build());
        rule.getBucketToRuleMap().put(RuleBucketName.A.getName(), ruleA);

        Map<String, Restriction> ruleC = new HashMap<>();
        ruleC.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                Restriction.builder().let(BusinessEntity.Account, "CompanyName").in("H", "N").build());
        rule.getBucketToRuleMap().put(RuleBucketName.C.getName(), ruleC);

        model.setRatingRule(rule);

        return model;
    }

    private List<String> createSelectedBuckets() {
        List<String> selectedBuckets = new ArrayList<>();
        selectedBuckets.add(RuleBucketName.A.getName());
        selectedBuckets.add(RuleBucketName.A_MINUS.getName());
        selectedBuckets.add(RuleBucketName.C.getName());
        return selectedBuckets;
    }

}
