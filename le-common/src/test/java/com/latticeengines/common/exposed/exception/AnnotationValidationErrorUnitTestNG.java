package com.latticeengines.common.exposed.exception;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.expection.AnnotationValidationError;

public class AnnotationValidationErrorUnitTestNG {

    @Test(groups = "unit")
    public void twoErrorsWithSameFieldsShouldBeEqual() {
        AnnotationValidationError error1 = new AnnotationValidationError("attr1", "AllowedValuesAnnotationValidator");
        AnnotationValidationError error2 = new AnnotationValidationError("attr1", "AllowedValuesAnnotationValidator");
        AnnotationValidationError error3 = new AnnotationValidationError("attr3", "NotNullAnnotationValidator");
        assertTrue(error1.equals(error2));
        assertTrue(error1.hashCode() == error2.hashCode());
        assertFalse(error1.equals(error3));
        assertFalse(error1.hashCode() == error3.hashCode());
    }

}
