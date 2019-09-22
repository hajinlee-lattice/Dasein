package com.latticeengines.common.exposed.util;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

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

    @Test(groups = "unit", dataProvider = "matchFieldValidation")
    private void testMatchFieldValidation(String str, boolean expectedValid) {
        Assert.assertEquals(ValidationUtils.isValidMatchFieldValue(str), expectedValid,
                String.format("%s should be a(n) %s match field value", str, expectedValid ? "Valid" : "Invalid"));
    }

    @DataProvider(name = "matchFieldValidation")
    private Object[][] provideMatchFieldValidationTestData() {
        return new Object[][] { //
                /*-
                 * all kinds of blank/null str is fine
                 */
                { null, true }, //
                { "    ", true }, //
                { "", true }, //
                /*-
                 * Valid values
                 */
                { "account_123", true }, //
                { " acco   unt|1   23", true }, //
                { "12345", true }, //
                { " aabb", true }, //
                { " AbCDEfgh  123 4", true }, //
                { "ZZZZZ", true }, //
                { "abc@gmail.com", true }, //
                { "Google Inc.", true }, //
                { "facebook.com", true }, //
                { "USA", true }, //
                { "CA", true }, //
                { "CA|123", true }, // single pipe
                { "(123)-123-1234", true }, //
                { "333-333-3333", true }, //
                { "Ronnie O'sullivan", true }, //
                /*-
                 * Invalid values
                 */
                { "abc||123  ", false }, // double pipe
                { "abc|1|2|||3  ", false }, // double pipe
                { "#some awesome hashtag", false }, // hashtag
                { "123:456:789", false }, // colon
                { "abc#123:||:AAd#", false }, // mix
                { randomAlphanumeric(1000), false }, // exceed length limit
        }; //
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
