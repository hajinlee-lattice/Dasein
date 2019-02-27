package com.latticeengines.pls.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

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
            fail("should have thrown exception");
        } catch (Exception expected) {
            // ignore
        }
//        try {
//            ValidateEnrichAttributesUtils.validateEnrichAttributes(str3);
//            fail("should have thrown exception");
//        } catch (Exception expected) {
//            // ignore
//        }
        try {
            ValidateEnrichAttributesUtils.validateEnrichAttributes(str4);
            fail("should have thrown exception");
        } catch (Exception expected) {
            // ignore
        }
    }
}
