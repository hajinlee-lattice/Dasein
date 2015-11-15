package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.util.Properties;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;


public class AlgorithmUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
    	Algorithm algorithm = new DecisionTreeAlgorithm();
        algorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=128 QUEUE=default");
        algorithm.setAlgorithmProperties("SAMPLESIZE=1000 NUMITERS=100");
        String jsonString = algorithm.toString();
        Algorithm deserializedAlgorithm = JsonUtils.deserialize(jsonString, DecisionTreeAlgorithm.class);
        assertEquals(deserializedAlgorithm.toString(), jsonString);
        Properties containerProps = deserializedAlgorithm.getContainerProps();
        assertEquals(containerProps.getProperty("VIRTUALCORES"), "1");
        assertEquals(containerProps.getProperty("MEMORY"), "128");
        assertEquals(containerProps.getProperty("QUEUE"), "default");
        Properties algorithmProps = deserializedAlgorithm.getAlgorithmProps();
        assertEquals(algorithmProps.getProperty("SAMPLESIZE"), "1000");
        assertEquals(algorithmProps.getProperty("NUMITERS"), "100");
        assertEquals(deserializedAlgorithm.getMapperSize(), "1");
    }
}
