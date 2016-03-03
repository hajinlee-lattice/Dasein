package com.latticeengines.domain.exposed.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.validators.InputValidator;

public class ValidatorUnitTestNG {
    @Test(groups = "unit")
    public void testSerDe() {
        InputValidatorWrapper wrapper = new InputValidatorWrapper();
        TestValidator validator = new TestValidator();
        validator.setTest("foo");
        wrapper.setValidator(validator);
        InputValidator retrieved = wrapper.getValidator();
        TestValidator casted = (TestValidator) retrieved;
        assertNotNull(casted);
        assertEquals(casted.getTest(), "foo");
    }
}
