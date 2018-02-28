package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

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
                "ai_i_2tncmeqhkllu3pnjrjza", "engine_ai3", //
                "ai_jpbt0qxzq72_kd4cd7f_ga", "engine_ai4"));


        config.setEvModelIds(Collections.singletonList("ai_i_2tncmeqhkllu3pnjrjza"));
        config.setAiModelIds(Arrays.asList("ai_jpbt0qxzq72_kd4cd7f_ga", "ai_i_2tncmeqhkllu3pnjrjza"));
        config.setAiSourceIdx(0);
        config.setRuleSourceIdx(1);

        TransformationFlowParameters params = new TransformationFlowParameters();
        params.setConfJson(JsonUtils.serialize(config));
        params.setBaseTables(Arrays.asList("AIBased", "RuleBased"));

        return params;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }

}
