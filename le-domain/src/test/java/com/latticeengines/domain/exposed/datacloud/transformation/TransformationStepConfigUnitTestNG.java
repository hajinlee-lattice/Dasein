package com.latticeengines.domain.exposed.datacloud.transformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.metadata.Category;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

public class TransformationStepConfigUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() {
        ObjectMapper OM = new ObjectMapper();
        TransformationStepConfig config = new TransformationStepConfig();
        ObjectNode objectNode = OM.createObjectNode();
        objectNode.put("StringField", "StringValue");
        TransformationFlowParameters.EngineConfiguration engineConfiguration = new TransformationFlowParameters.EngineConfiguration();
        engineConfiguration.setEngine("flink");
        objectNode.put("EngineConfig", OM.valueToTree(engineConfiguration));
        config.setConfiguration(JsonUtils.serialize(objectNode));

        String serialized = JsonUtils.serialize(config);
        TransformationStepConfig deser = JsonUtils.deserialize(serialized, TransformationStepConfig.class);

    }

}
