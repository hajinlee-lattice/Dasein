package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ModelIdUtilsUnitTestNG {

    private static final String CORRECT_FORMATTED_ID = "ms__8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a-PLSModel";
    private static final String INCORRECT_FORMATTED_ID = "someRamdomString";

    @Test(groups = "unit")
    public void parseCorrectFormattedModelId() {
        String uuid = ModelIdUtils.extractUuid(CORRECT_FORMATTED_ID);
        assertEquals(uuid, "8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a");
    }

    @Test(groups = "unit")
    public void parseIncorrectFormattedModelId() {
        try {
            ModelIdUtils.extractUuid(INCORRECT_FORMATTED_ID);
            Assert.fail("Should have thrown an exception.");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
