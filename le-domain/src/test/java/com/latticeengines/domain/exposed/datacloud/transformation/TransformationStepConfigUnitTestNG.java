package com.latticeengines.domain.exposed.datacloud.transformation;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class TransformationStepConfigUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() {
        ObjectMapper om = new ObjectMapper();
        TransformationStepConfig config = new TransformationStepConfig();
        ObjectNode objectNode = om.createObjectNode();
        objectNode.put("StringField", "StringValue");
        TransformationFlowParameters.EngineConfiguration engineConfiguration = new TransformationFlowParameters.EngineConfiguration();
        engineConfiguration.setEngine("flink");
        objectNode.set("EngineConfig", om.valueToTree(engineConfiguration));
        config.setConfiguration(JsonUtils.serialize(objectNode));

        String serialized = JsonUtils.serialize(config);
        TransformationStepConfig deser = JsonUtils.deserialize(serialized, TransformationStepConfig.class);
        Assert.assertNotNull(deser);
    }


    @Test(groups = "unit")
    public void testDeSerInteractiveStrategy() {
        IterativeStepConfig.ConvergeOnCount convergeOnCount = new IterativeStepConfig.ConvergeOnCount();
        convergeOnCount.setCountDiff(0);
        System.out.println(JsonUtils.serialize(convergeOnCount));
    }

}
