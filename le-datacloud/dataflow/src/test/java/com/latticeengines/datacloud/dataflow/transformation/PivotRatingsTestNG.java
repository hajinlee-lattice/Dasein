package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;

public class PivotRatingsTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return PivotRatings.BEAN_NAME;
    }

    @Override
    protected String getScenarioName() {
        return "CDL";
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        PivotRatingsConfig config = new PivotRatingsConfig();
        config.setIdAttrsMap(ImmutableMap.of( //
                "rule_y2dvrdmfqc2f6tsl1uqc7g", "engine_rule1", //
                "rule_yt0b97azssea_i_gch6cww", "engine_rule2", //
                "ai_dy4cicoutpw_bmraclvlfg", "engine_ai3", //
                "ai_lhydyrfaq52ro_pke_t8aa", "engine_ai4"));

        config.setEvModelIds(Collections.singletonList("ai_dy4cicoutpw_bmraclvlfg"));
        config.setAiModelIds(Arrays.asList("ai_dy4cicoutpw_bmraclvlfg", "ai_lhydyrfaq52ro_pke_t8aa"));
        config.setAiSourceIdx(0);
        config.setRuleSourceIdx(1);

        TransformationFlowParameters params = new TransformationFlowParameters();
        params.setConfJson(JsonUtils.serialize(config));
        params.setBaseTables(Arrays.asList("AIBased", "RuleBased"));

        return params;
    }

    @SuppressWarnings("unused")
    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Table table = getOutputSchema();
        List<String> attrs = Arrays.asList(table.getAttributeNames());
        // the following three names are always generated in Join operation
        Assert.assertFalse(attrs.contains("ai_score__AccountId"));
        Assert.assertFalse(attrs.contains("ai_ev__AccountId"));
        Assert.assertFalse(attrs.contains("ai_rating__ai_score__ai_ev__AccountId"));
        Assert.assertTrue(attrs.contains("engine_rule1"));
        Assert.assertFalse(attrs.contains("engine_rule1_ev"));
        Assert.assertFalse(attrs.contains("engine_rule1_pv"));
        Assert.assertFalse(attrs.contains("engine_rule1_score"));
        Assert.assertTrue(attrs.contains("engine_rule2"));
        Assert.assertFalse(attrs.contains("engine_rule2_ev"));
        Assert.assertFalse(attrs.contains("engine_rule2_pv"));
        Assert.assertFalse(attrs.contains("engine_rule2_score"));
        Assert.assertTrue(attrs.contains("engine_ai3"));
        Assert.assertTrue(attrs.contains("engine_ai3_ev"));
        Assert.assertTrue(attrs.contains("engine_ai3_pv"));
        Assert.assertTrue(attrs.contains("engine_ai3_score"));
        Assert.assertTrue(attrs.contains("engine_ai4"));
        Assert.assertFalse(attrs.contains("engine_ai4_ev"));
        Assert.assertFalse(attrs.contains("engine_ai4_pv"));
        Assert.assertTrue(attrs.contains("engine_ai4_score"));
        for (GenericRecord record : records) {
//             System.out.println(record);
        }
    }

}
