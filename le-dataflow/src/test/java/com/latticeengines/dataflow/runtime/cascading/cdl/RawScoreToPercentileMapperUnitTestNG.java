package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.io.InputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class RawScoreToPercentileMapperUnitTestNG {
    @Test(groups = "unit")
    public void testMapping() {
        ScoreDerivation scoreDerivation = loadScoreDerivationFromResource(
            "com/latticeengines/dataflow/runtime/cascading/cdl/scorederivation.json");

        Assert.assertNotNull(scoreDerivation);
        RawScoreToPercentileMapper mapper = new RawScoreToPercentileMapper(scoreDerivation);

        double[] rawScores = {0.00760871, 0.008451298, 0.8};
        int[] expectedResults = {5, 15, 99};

        for (int i = 0; i < rawScores.length; ++i) {
            Assert.assertEquals(mapper.map(rawScores[i]), expectedResults[i]);
        }
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testEmptyScoreDerivation() {
        ScoreDerivation scoreDerivation = loadScoreDerivationFromResource(
            "com/latticeengines/dataflow/runtime/cascading/cdl/scorederivation1.json");

        Assert.assertNotNull(scoreDerivation);
        new RawScoreToPercentileMapper(scoreDerivation);
    }

    private ScoreDerivation loadScoreDerivationFromResource(String resourceName) {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
            ScoreDerivation sd = JsonUtils.deserialize(inputStream, ScoreDerivation.class);
            return sd;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load json resource" + resourceName, e);
        }

    }
}
