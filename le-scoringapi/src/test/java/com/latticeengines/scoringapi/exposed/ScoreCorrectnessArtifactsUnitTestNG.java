package com.latticeengines.scoringapi.exposed;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoringapi.FieldSchema;

public class ScoreCorrectnessArtifactsUnitTestNG {

    private ScoreCorrectnessArtifacts artifacts;

    private String expectedRecords = "1:0.2";
    private String scoredTxt = "1:0.2";
    private String idName = "id";
    private String path = "/directory/path";
    private Map<String, FieldSchema> mergedFields = new HashMap<>();

    @Test(groups = "unit")
    public void testBasicOperation() {
        artifacts = new ScoreCorrectnessArtifacts();
        artifacts.setExpectedRecords(expectedRecords);
        artifacts.setScoredTxt(scoredTxt);
        artifacts.setIdField(idName);
        artifacts.setPathToSamplesAvro(path);
        artifacts.setFieldSchemas(mergedFields);
        Assert.assertEquals(artifacts.getExpectedRecords(), expectedRecords);
        Assert.assertEquals(artifacts.getScoredTxt(), scoredTxt);
        Assert.assertEquals(artifacts.getIdField(), idName);
        Assert.assertEquals(artifacts.getPathToSamplesAvro(), path);
    }

}
