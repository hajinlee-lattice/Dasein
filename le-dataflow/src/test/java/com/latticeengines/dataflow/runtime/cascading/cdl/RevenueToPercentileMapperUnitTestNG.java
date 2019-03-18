package com.latticeengines.dataflow.runtime.cascading.cdl;

import java.io.InputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.EVScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;

public class RevenueToPercentileMapperUnitTestNG {
    @Test(groups = "unit")
    public void testPredictedRevenueMapping() {
        ScoreDerivation scoreDerivation = loadScoreDerivationFromResource(
            "com/latticeengines/dataflow/runtime/cascading/cdl/evscorederivation.json").getRevenueScoreDerivation();

        Assert.assertNotNull(scoreDerivation);
        RevenueToPercentileMapper mapper = new RevenueToPercentileMapper(scoreDerivation);

        double[] rawScores = {100.0, 660.0, 9600.};
        int[] expectedResults = {5, 15, 99};

        for (int i = 0; i < rawScores.length; ++i) {
            Assert.assertEquals(mapper.map(rawScores[i]), expectedResults[i]);
        }
    }

    @Test(groups = "unit")
    public void testExpectedRevenueMapping() {
        ScoreDerivation scoreDerivation = loadScoreDerivationFromResource(
            "com/latticeengines/dataflow/runtime/cascading/cdl/evscorederivation.json").getEVScoreDerivation();

        Assert.assertNotNull(scoreDerivation);
        RevenueToPercentileMapper mapper = new RevenueToPercentileMapper(scoreDerivation);

        double[] rawScores = {1.8, 2.70, 2500.};
        int[] expectedResults = {5, 15, 99};

        for (int i = 0; i < rawScores.length; ++i) {
            Assert.assertEquals(mapper.map(rawScores[i]), expectedResults[i]);
        }
    }

    private EVScoreDerivation loadScoreDerivationFromResource(String resourceName) {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
            EVScoreDerivation sd = JsonUtils.deserialize(inputStream, EVScoreDerivation.class);
            return sd;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load json resource" + resourceName, e);
        }

    }
}
