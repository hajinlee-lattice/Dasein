package com.latticeengines.apps.dcp.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;

public class MatchRuleServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private MatchRuleService matchRuleService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCRUD() {
        // Create Base Rule
        MatchRule matchRule = new MatchRule();
        matchRule.setSourceId("Source_1231231");
        matchRule.setDisplayName("Match Rule 1");
        matchRule.setRuleType(MatchRuleRecord.RuleType.BASE_RULE);

        matchRule.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.OutOfBusiness));
        DplusMatchRule dplusMatchRule = new DplusMatchRule(8, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(4, 6, Arrays.asList("C", "D"));

        matchRule.setAcceptCriterion(dplusMatchRule.getAcceptCriterion());
        matchRule.setReviewCriterion(dplusMatchRule.getReviewCriterion());

        MatchRule createRule = matchRuleService.createMatchRule(mainCustomerSpace, matchRule);
        Assert.assertNotNull(createRule);
        Assert.assertEquals(createRule.getVersionId().intValue(), 1);
        Assert.assertEquals(createRule.getState(), MatchRuleRecord.State.ACTIVE);
        String baseMatchRuleId = createRule.getMatchRuleId();
        Assert.assertTrue(StringUtils.isNotBlank(baseMatchRuleId));

        // Create SpecialRule
        MatchRule specialRule = new MatchRule();
        specialRule.setSourceId("Source_1231231");
        specialRule.setDisplayName("Match Rule 2");
        specialRule.setRuleType(MatchRuleRecord.RuleType.SPECIAL_RULE);
        specialRule.setMatchKey(MatchKey.Country);
        specialRule.setAllowedValues(Arrays.asList("USA", "UK"));
        specialRule.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.Unreachable));
        DplusMatchRule dplusMatchRule2 = new DplusMatchRule(4, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(1, 10, Arrays.asList("C", "D"));

        specialRule.setAcceptCriterion(dplusMatchRule2.getAcceptCriterion());

        specialRule = matchRuleService.createMatchRule(mainCustomerSpace, specialRule);
        Assert.assertNotNull(specialRule);
        Assert.assertEquals(specialRule.getVersionId().intValue(), 1);
        Assert.assertEquals(specialRule.getState(), MatchRuleRecord.State.ACTIVE);
        String specialMatchRuleId = specialRule.getMatchRuleId();
        Assert.assertTrue(StringUtils.isNotBlank(specialMatchRuleId));

        // Create one more SpecialRule
        MatchRule specialRule2 = new MatchRule();
        specialRule2.setSourceId("Source_1231231");
        specialRule2.setDisplayName("Match Rule 3");
        specialRule2.setRuleType(MatchRuleRecord.RuleType.SPECIAL_RULE);
        specialRule2.setMatchKey(MatchKey.Country);
        specialRule2.setAllowedValues(Arrays.asList("USA", "UK"));
        specialRule2.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.Unreachable));
        DplusMatchRule dplusMatchRule3 = new DplusMatchRule(2, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(1, 10, Arrays.asList("E", "F"));

        specialRule2.setAcceptCriterion(dplusMatchRule3.getAcceptCriterion());
        specialRule2.setReviewCriterion(dplusMatchRule3.getReviewCriterion());

        specialRule2 = matchRuleService.createMatchRule(mainCustomerSpace, specialRule2);
        Assert.assertNotNull(specialRule2);
        Assert.assertEquals(specialRule2.getVersionId().intValue(), 1);
        Assert.assertEquals(specialRule2.getState(), MatchRuleRecord.State.ACTIVE);
        String specialMatchRuleId2 = specialRule2.getMatchRuleId();
        Assert.assertTrue(StringUtils.isNotBlank(specialMatchRuleId2));

        // Check list
        List<MatchRule> matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, "Source_1231231", false,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 3);

        // Update special rule 2
        specialRule2.setDisplayName("Updated Match Rule 3");
        specialRule2 = matchRuleService.updateMatchRule(mainCustomerSpace, specialRule2);
        Assert.assertEquals(specialRule2.getVersionId().intValue(), 1);
        Assert.assertEquals(specialRule2.getDisplayName(), "Updated Match Rule 3");

        // Check list again
        matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, "Source_1231231", false,
                true);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 3);

        // update special rule 2 again
        specialRule2.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.OutOfBusiness, ExclusionCriterion.Unreachable));
        specialRule2 = matchRuleService.updateMatchRule(mainCustomerSpace, specialRule2);
        Assert.assertEquals(specialRule2.getVersionId().intValue(), 2);

        // Check list again
        matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, "Source_1231231", false,
                true);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 4);

        matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, "Source_1231231", false,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 3);

        // archive special rule 2
        matchRuleService.archiveMatchRule(mainCustomerSpace, specialMatchRuleId2);

        // Check list
        matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, "Source_1231231", false,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 2);

        matchRules = matchRuleService.getMatchRuleList(mainCustomerSpace, "Source_1231231", true,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 4);

        // Check match Config
        MatchRuleConfiguration matchConfig = matchRuleService.getMatchConfig(mainCustomerSpace, "Source_1231231");
        Assert.assertNotNull(matchConfig);
        Assert.assertNotNull(matchConfig.getBaseRule());
        Assert.assertEquals(matchConfig.getBaseRule().getVersionId().intValue(), 1);
        Assert.assertEquals(matchConfig.getBaseRule().getMatchRuleId(), baseMatchRuleId);

        Assert.assertEquals(CollectionUtils.size(matchConfig.getSpecialRules()), 1);
        Assert.assertEquals(matchConfig.getSpecialRules().get(0).getMatchRuleId(), specialMatchRuleId);

        // Exception when delete BaseRule
        Assert.assertThrows(() -> matchRuleService.archiveMatchRule(mainCustomerSpace, baseMatchRuleId));

        MatchRule anotherBaseRule = new MatchRule();
        anotherBaseRule.setSourceId("Source_1231231");
        anotherBaseRule.setDisplayName("Match Rule 1");
        anotherBaseRule.setRuleType(MatchRuleRecord.RuleType.BASE_RULE);

        anotherBaseRule.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.OutOfBusiness));

        anotherBaseRule.setAcceptCriterion(dplusMatchRule.getAcceptCriterion());
        anotherBaseRule.setReviewCriterion(dplusMatchRule.getReviewCriterion());

        Assert.assertThrows(() -> matchRuleService.createMatchRule(mainCustomerSpace, anotherBaseRule));

    }
}
