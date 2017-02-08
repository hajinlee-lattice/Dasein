package com.latticeengines.serviceflows.workflow.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class ModelingServiceExecutorUnitTestNG {

    @Test(groups = "unit")
    public void testGetEnabledRulesAsPipelineProp() {
        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        ModelingServiceExecutor modelingServiceExecutor = new ModelingServiceExecutor(bldr);

        List<DataRule> dataRules = new ArrayList<>();
        DataRule ruleA = new DataRule("RuleA");
        ruleA.setMandatoryRemoval(true);
        ruleA.setEnabled(true);
        ruleA.setFlaggedColumnNames(Arrays.asList("ColA", "ColB", "ColC"));
        ruleA.setProperties(ImmutableMap.of("k1", "v1", "k2", "v2"));
        ruleA.addCustomerPredictor("ColA");
        dataRules.add(ruleA);
        DataRule ruleB = new DataRule("RuleB");
        ruleB.setMandatoryRemoval(true);
        ruleB.setEnabled(false);
        dataRules.add(ruleB);
        DataRule ruleC = new DataRule("RuleC");
        ruleC.setMandatoryRemoval(false);
        ruleC.setEnabled(true);
        dataRules.add(ruleC);

        String result = modelingServiceExecutor.getEnabledRulesAsPipelineProp(dataRules);
        verifyEnabledRules(result);
    }

    @SuppressWarnings("unchecked")
    private void verifyEnabledRules(String result) {
        // verify result is
        // "remediatedatarulesstep.enabledRules={"RuleA":["ColA","ColB","ColC"]}
        // remediatedatarulesstep.customerPredictors=["ColA"]"
        String[] properties = result.split(" ");
        String property1 = properties[0].split("=")[0];
        String enabledRules = properties[0].split("=")[1];
        String property2 = properties[1].split("=")[0];
        String customerPredictors = properties[1].split("=")[1];
        Assert.assertEquals(property1, "remediatedatarulesstep.enabledRules");
        Assert.assertEquals(property2, "remediatedatarulesstep.customerPredictors");
        Map<String, List<String>> enabledRulesMap = new HashMap<>();
        enabledRulesMap = JsonUtils.deserialize(enabledRules, Map.class, true);

        Assert.assertEquals(enabledRulesMap.size(), 1);
        Assert.assertTrue(enabledRulesMap.containsKey("RuleA"));

        List<String> cols = enabledRulesMap.get("RuleA");
        Assert.assertEquals(cols.size(), 3);
        Assert.assertTrue(cols.contains("ColA"));
        Assert.assertTrue(cols.contains("ColB"));
        Assert.assertTrue(cols.contains("ColC"));

        List<String> customerPredictorsList = new ArrayList<>();
        customerPredictorsList = JsonUtils.deserialize(customerPredictors, List.class, true);

        Assert.assertEquals(customerPredictorsList.size(), 1);
        Assert.assertTrue(customerPredictorsList.contains("ColA"));
    }

}
