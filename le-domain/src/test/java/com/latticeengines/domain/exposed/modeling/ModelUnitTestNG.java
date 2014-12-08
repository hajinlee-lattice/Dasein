package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;

public class ModelUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64");
        decisionTreeAlgorithm.setSampleName("s1");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setTable("DELL_EVENT_TABLE_TEST");
        model.setFeaturesList(Arrays.<String> asList(new String[] {
                "Column5", //
                "Column6", //
                "Column7", //
                "Column8", //
                "Column9", //
                "Column10" }));
        model.setTargetsList(Arrays.<String> asList(new String[] { "Event_Latitude_Customer" }));
        model.setCustomer("DELL");
        model.setDataFormat("avro");
        
        String modelStr = model.toString();
        
        Model deserializedModel = JsonUtils.deserialize(modelStr, Model.class);
        assertEquals(deserializedModel.getModelDefinition().getAlgorithms().size(), 2);
        assertEquals(deserializedModel.getModelDefinition().getAlgorithms().get(0).getPriority(), decisionTreeAlgorithm.getPriority());
        assertEquals(deserializedModel.getModelDefinition().getAlgorithms().get(1).getPriority(), logisticRegressionAlgorithm.getPriority());
    }
}
