package com.latticeengines.serviceflows.workflow.core;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.modelreview.DataRule;

public class ModelingServiceExecutorUnitTestNG {

    @Test(groups = "unit")
    public void testGetEnabledRulesAsPipelineProp() {
        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        ModelingServiceExecutor modelingServiceExecutor = new ModelingServiceExecutor(bldr);

        List<DataRule> dataRules = new ArrayList<>();
        DataRule ruleA = new DataRule("RuleA");
        ruleA.setMandatoryRemoval(false);
        ruleA.setEnabled(true);
        ruleA.setFlaggedColumnNames(Arrays.asList("ColA", "ColB", "ColC"));
        ruleA.setProperties(ImmutableMap.of("k1", "v1", "k2", "v2"));
        dataRules.add(ruleA);
        DataRule ruleB = new DataRule("RuleB");
        ruleB.setMandatoryRemoval(false);
        ruleB.setEnabled(false);
        dataRules.add(ruleB);
        DataRule ruleC = new DataRule("RuleC");
        ruleC.setMandatoryRemoval(false);
        ruleC.setEnabled(true);
        dataRules.add(ruleC);

        String result = modelingServiceExecutor.getEnabledRulesAsPipelineProp(dataRules);
        assertEquals(result,
                "remediatedatarulesstep.enabledRules={\"RuleA\":[\"ColA\",\"ColB\",\"ColC\"],\"RuleC\":[]}");
    }

}
