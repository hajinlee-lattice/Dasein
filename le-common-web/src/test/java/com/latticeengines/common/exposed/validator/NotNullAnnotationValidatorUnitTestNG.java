package com.latticeengines.common.exposed.validator;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.annotation.Annotation;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.validator.impl.NotNullAnnotationValidator;

public class NotNullAnnotationValidatorUnitTestNG {
    private static final String NOT_NULL_VALUE = "NOT_NULL_VALUE";

    private NotNullAnnotationValidator notNullAnnotationValidator;

    private static class ObjectWithNonNullField {
        @NotNull
        private String attr;
    }

    @BeforeClass(groups = "unit")
    public void setup() {
        this.notNullAnnotationValidator = new NotNullAnnotationValidator();
    }

    @Test(groups = "unit")
    public void objectWithNonNullField_fieldHasNotNullValue_validatorReturnsTrue() throws NoSuchFieldException {
        Annotation notNullAnnotation = ObjectWithNonNullField.class.getDeclaredField("attr").getAnnotation(
                NotNull.class);
        assertTrue(this.notNullAnnotationValidator.validate(NOT_NULL_VALUE, notNullAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithNonNullField_fieldHasNullValue_validatorReturnsFalse() throws NoSuchFieldException {
        Annotation notNullAnnotation = ObjectWithNonNullField.class.getDeclaredField("attr").getAnnotation(
                NotNull.class);
        assertFalse(this.notNullAnnotationValidator.validate(null, notNullAnnotation));
    }
}
