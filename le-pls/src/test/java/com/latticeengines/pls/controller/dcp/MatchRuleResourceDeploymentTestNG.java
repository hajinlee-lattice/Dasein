package com.latticeengines.pls.controller.dcp;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestMatchRuleProxy;

public class MatchRuleResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private TestMatchRuleProxy testMatchRuleProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testMatchRuleProxy);
    }

    @Test(groups = "deployment")
    public void testGetAndUpdateMatchRule() {
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

        MatchRule createRule = testMatchRuleProxy.createMatchRule(matchRule);
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
        specialRule.setAllowedValues(Arrays.asList("US", "GB"));
        specialRule.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.Unreachable));
        DplusMatchRule dplusMatchRule2 = new DplusMatchRule(4, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(1, 10, Arrays.asList("C", "D"));

        specialRule.setAcceptCriterion(dplusMatchRule2.getAcceptCriterion());

        specialRule = testMatchRuleProxy.createMatchRule(specialRule);
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
        specialRule2.setAllowedValues(Arrays.asList("CA", "CC"));
        specialRule2.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.Unreachable));
        DplusMatchRule dplusMatchRule3 = new DplusMatchRule(2, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(1, 10, Arrays.asList("E", "F"));

        specialRule2.setAcceptCriterion(dplusMatchRule3.getAcceptCriterion());
        specialRule2.setReviewCriterion(dplusMatchRule3.getReviewCriterion());

        specialRule2 = testMatchRuleProxy.createMatchRule(specialRule2);
        Assert.assertNotNull(specialRule2);
        Assert.assertEquals(specialRule2.getVersionId().intValue(), 1);
        Assert.assertEquals(specialRule2.getState(), MatchRuleRecord.State.ACTIVE);
        String specialMatchRuleId2 = specialRule2.getMatchRuleId();
        Assert.assertTrue(StringUtils.isNotBlank(specialMatchRuleId2));

        // Check list, expected 3 active rules.(1 base + 2 special)
        List<MatchRule> matchRules = testMatchRuleProxy.getMatchRuleList("Source_1231231", false,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 3);

        // Update special rule 2
        specialRule2.setDisplayName("Updated Match Rule 3");
        specialRule2 = testMatchRuleProxy.updateMatchRule(specialRule2);
        Assert.assertEquals(specialRule2.getVersionId().intValue(), 1);
        Assert.assertEquals(specialRule2.getDisplayName(), "Updated Match Rule 3");

        // Check list again
        matchRules = testMatchRuleProxy.getMatchRuleList("Source_1231231", false,
                true);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 3);

        // update special rule 2 again
        specialRule2.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.OutOfBusiness, ExclusionCriterion.Unreachable));
        specialRule2 = testMatchRuleProxy.updateMatchRule(specialRule2);
        Assert.assertEquals(specialRule2.getVersionId().intValue(), 2);

        // Check list again, expected 4 rules, (1 active base + 2 active special + 1 inactive special)
        matchRules = testMatchRuleProxy.getMatchRuleList("Source_1231231", false,
                true);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 4);

        matchRules = testMatchRuleProxy.getMatchRuleList("Source_1231231", false,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 3);

        // archive special rule 2
        testMatchRuleProxy.deleteMatchRule(specialMatchRuleId2);

        SleepUtils.sleep(1000L);

        // Check list, expected 2 active rules (1 base + 1 special)
        matchRules = testMatchRuleProxy.getMatchRuleList("Source_1231231", false,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 2);

        // Check list, expected 4 rules, (1 active base + 1 active special + 2 archived special)
        matchRules = testMatchRuleProxy.getMatchRuleList("Source_1231231", true,
                false);
        Assert.assertNotNull(matchRules);
        Assert.assertEquals(matchRules.size(), 4);

        // Exception when delete BaseRule
        Assert.assertThrows(() -> testMatchRuleProxy.deleteMatchRule(baseMatchRuleId));

        // Exception expected when create another base rule under same source.
        MatchRule anotherBaseRule = new MatchRule();
        anotherBaseRule.setSourceId("Source_1231231");
        anotherBaseRule.setDisplayName("Match Rule 1");
        anotherBaseRule.setRuleType(MatchRuleRecord.RuleType.BASE_RULE);

        anotherBaseRule.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.OutOfBusiness));

        anotherBaseRule.setAcceptCriterion(dplusMatchRule.getAcceptCriterion());
        anotherBaseRule.setReviewCriterion(dplusMatchRule.getReviewCriterion());

        Assert.assertThrows(() -> testMatchRuleProxy.createMatchRule(anotherBaseRule));
    }
}
