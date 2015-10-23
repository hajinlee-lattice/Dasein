package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PredictorStatusUnitTestNG {

    @Test(groups = "unit")
    public void testGetflipedStatusCode() {
        assertTrue(PredictorStatus.getflippedStatusCode(true).equals("0"));
        assertTrue(PredictorStatus.getflippedStatusCode(false).equals("1"));
    }
}
