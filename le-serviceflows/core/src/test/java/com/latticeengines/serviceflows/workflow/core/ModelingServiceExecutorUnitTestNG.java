package com.latticeengines.serviceflows.workflow.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        verifyEnabledRules(result);
    }

    private void verifyEnabledRules(String result) {
        // verify result is "remediatedatarulesstep.enabledRules={\"RuleA\":[\"ColA\",\"ColB\",\"ColC\"],\"RuleC\":[]}"
        Assert.assertTrue(result.startsWith("remediatedatarulesstep.enabledRules="));
        String jsonStr = result.replace("remediatedatarulesstep.enabledRules=", "");
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<Map<String, List<String>>> typeRef = new TypeReference<Map<String, List<String>>>(){};
        Map<String, List<String>> map = new HashMap<>();
        try {
            map = objectMapper.readValue(jsonStr, typeRef);
        } catch (IOException e) {
            Assert.fail("Failed to deserialize json string [" + jsonStr + "]", e);
        }

        Assert.assertEquals(map.size(), 2);
        Assert.assertTrue(map.containsKey("RuleA"));
        Assert.assertTrue(map.containsKey("RuleC"));

        List<String> cols = map.get("RuleA");
        Assert.assertEquals(cols.size(), 3);
        Assert.assertTrue(cols.contains("ColA"));
        Assert.assertTrue(cols.contains("ColB"));
        Assert.assertTrue(cols.contains("ColC"));
    }

}
