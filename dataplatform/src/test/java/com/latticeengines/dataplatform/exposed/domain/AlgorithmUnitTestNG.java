package com.latticeengines.dataplatform.exposed.domain;

import static org.testng.Assert.assertEquals;

import java.util.Properties;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.util.JsonHelper;

public class AlgorithmUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        Algorithm algorithm = new Algorithm();
        algorithm.setName("nn");
        algorithm.setScript("nn_train.py");
        algorithm
                .setContainerProperties("VIRTUALCORES=1 MEMORY=128 QUEUE=default");
        algorithm.setAlgorithmProperties("SAMPLESIZE=1000 NUMITERS=100");
        String jsonString = algorithm.toString();
        Algorithm deserializedAlgorithm = JsonHelper.deserialize(jsonString,
                Algorithm.class);
        assertEquals(deserializedAlgorithm.toString(), jsonString);
        Properties containerProps = deserializedAlgorithm.getContainerProps();
        assertEquals(containerProps.getProperty("VIRTUALCORES"), "1");
        assertEquals(containerProps.getProperty("MEMORY"), "128");
        assertEquals(containerProps.getProperty("QUEUE"), "default");
        Properties algorithmProps = deserializedAlgorithm.getAlgorithmProps();
        assertEquals(algorithmProps.getProperty("SAMPLESIZE"), "1000");
        assertEquals(algorithmProps.getProperty("NUMITERS"), "100");
    }
}
