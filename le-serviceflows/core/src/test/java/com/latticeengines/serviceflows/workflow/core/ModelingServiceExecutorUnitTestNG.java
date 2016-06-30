package com.latticeengines.serviceflows.workflow.core;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelreview.DataRule;

public class ModelingServiceExecutorUnitTestNG {

    @Test(groups = "unit")
    public void testGetEnabledRulesAsPipelineProp() {
        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        ModelingServiceExecutor modelingServiceExecutor = new ModelingServiceExecutor(bldr);

        List<DataRule> dataRules = new ArrayList<>();
        DataRule ruleA = new DataRule();
        ruleA.setName("RuleA");
        ruleA.setEnabled(true);
        dataRules.add(ruleA);
        DataRule ruleB = new DataRule();
        ruleB.setName("RuleB");
        ruleB.setEnabled(false);
        dataRules.add(ruleB);
        DataRule ruleC = new DataRule();
        ruleC.setName("RuleC");
        ruleC.setEnabled(true);
        dataRules.add(ruleC);

        String result = modelingServiceExecutor.getEnabledRulesAsPipelineProp(dataRules);
        assertEquals(result, "remediatedatarulestep.enabledrules=[\"RuleA\", \"RuleC\"]");
    }

}
