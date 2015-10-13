package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PredictorUnitTestNG {

    private Predictor predictor1;
    private Predictor predictor2;

    @BeforeClass
    public void setup() {
        ModelSummary summary = new ModelSummary();

        predictor1 = new Predictor();
        predictor1.setName("p1");
        predictor1.setDisplayName("p1");
        predictor1.setApprovedUsage("Model");
        predictor1.setCategory("external");
        predictor1.setFundamentalType("year");
        predictor1.setUncertaintyCoefficient(0.1);
        predictor1.setModelSummary(summary);
        predictor1.setTenantId(1L);

        predictor2 = new Predictor();
        predictor2.setName("p2");
        predictor2.setDisplayName("p2");
        predictor2.setApprovedUsage("Model");
        predictor2.setCategory("internal");
        predictor2.setFundamentalType("year");
        predictor2.setUncertaintyCoefficient(0.12);
        predictor2.setModelSummary(summary);
        predictor2.setTenantId(1L);
    }

    @Test(groups = "unit")
    public void testCompare() {
        assertTrue(predictor1.compareTo(predictor2) > 0);
    }
}
