package com.latticeengines.pls.controller.dcp;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
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
        List<MatchRule> matchRuleList = testMatchRuleProxy.getMatchRuleList("Source_testsrc", false, false);

        Assert.assertNotNull(matchRuleList);
        Assert.assertTrue(CollectionUtils.isNotEmpty(matchRuleList));
        System.out.println(JsonUtils.serialize(matchRuleList));
        MatchRule matchRule = matchRuleList.get(0);
        matchRule.setDisplayName("NewDisplayName");
        DplusMatchRule dplusMatchRule = new DplusMatchRule(8, 9, Arrays.asList("newA", "newB"));
        matchRule.setAcceptCriterion(dplusMatchRule.getAcceptCriterion());
        MatchRule newMatchRule = testMatchRuleProxy.updateMatchRule(matchRule);
        Assert.assertEquals(newMatchRule.getDisplayName(), "NewDisplayName");
        System.out.println(JsonUtils.serialize(newMatchRule));
    }
}
