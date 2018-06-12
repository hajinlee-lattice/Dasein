package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RatingEngineUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        for (int i = 0; i < 10; i++) {
            String engineId = RatingEngine.generateIdStr();
            Assert.assertEquals(RatingEngine.toEngineId(engineId), engineId);
            Assert.assertEquals(
                    RatingEngine.toEngineId(RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.Score)),
                    engineId);
        }
    }

}
