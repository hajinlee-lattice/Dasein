package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;

public class ValidationUtilsUnitTestNG {

    private BeanValidationService service;

    @BeforeClass(groups = "unit")
    public void setup() {
        service = new BeanValidationServiceImpl();
    }

    @Test(groups = "unit", dataProvider = "validationTestCase")
    public void testValidateObject(Object obj, String objectName, boolean expectedValid,
        Class<? extends Exception> expectedException) {
        try {
            ValidationUtils.check(service, obj, objectName);
            if (!expectedValid) {
                Assert.fail("Validation should fail");
            }
        } catch (Exception e) {
            if (expectedValid) {
                Assert.fail("Validation should succeed");
            }
            Assert.assertNotNull(e);
            Assert.assertTrue(expectedException.isAssignableFrom(e.getClass()));
            Assert.assertNotNull(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(objectName));
        }
    }

    @DataProvider(name = "validationTestCase")
    private Object[][] provideValidationTestCases() {
        return new Object[][] {
                // invalid objects
                { null, "Null object", false, NullPointerException.class },
                { new TestObj(null), "Object with null value", false, IllegalArgumentException.class },
                { new TestObj(""), "Object with empty string", false, IllegalArgumentException.class },
                // valid objects
                { new TestObj("   "), "Object with valid string", true, null },
                { new TestObj("Lattice"), "Object with valid string", true, null },
                { new TestObj("Lattice Engines"), "Object with valid string", true, null },
        };
    }

    private static class TestObj {
        @NotNull
        @NotEmptyString
        final String value; // simple field that should not be null and empty string
        TestObj(String value) {
            this.value = value;
        }
    }
}
