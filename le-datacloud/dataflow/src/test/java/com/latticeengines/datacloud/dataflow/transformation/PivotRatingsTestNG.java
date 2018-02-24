package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
                "rule_zhvo_e38rauwem5gmo9klg", "engine_1", //
                "rule_zov878m4rsip1bdbratpgq", "engine_2", //
                "ai_npzjijw6tx2b_aqfa9pc_w", "engine_3", //
                "ai_srev4nwqstmrjnhef82vga", "engine_4"));

        config.setEvModelIds(Collections.singletonList("ai_npzjijw6tx2b_aqfa9pc_w"));
        config.setAiModelIds(Arrays.asList("ai_npzjijw6tx2b_aqfa9pc_w", "ai_srev4nwqstmrjnhef82vga"));
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
            if (Arrays.asList("0012400001DNPi6AAH", "0012400001DO97UAAT") //
                    .contains(record.get("AccountId").toString())) {
                System.out.println(record);
            }
        }
    }

}
