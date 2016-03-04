package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class ValidateEnrichAttributeUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testValidation() {
        String str1 = "10";
        String str2 = "2.2";
        String str3 = "1001";
        String str4 = "-1";
        assertEquals(ValidateEnrichAttributesUtils.validateEnrichAttributes(str1), 10);
        try {
            ValidateEnrichAttributesUtils.validateEnrichAttributes(str2);
            assertFalse(true, "should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
        }
        try {
            ValidateEnrichAttributesUtils.validateEnrichAttributes(str3);
            assertFalse(true, "should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
        }
        try {
            ValidateEnrichAttributesUtils.validateEnrichAttributes(str4);
            assertFalse(true, "should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
        }
    }
}
