package com.latticeengines.common.exposed.validator;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.annotation.Annotation;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.impl.NotEmptyStringAnnotationValidator;

public class NotEmptyStringAnnotationValidatorUnitTestNG {

    private static final String NOT_EMPTY_STRING = "notEmptyString";
    private NotEmptyStringAnnotationValidator notEmptyStringAnnotationValidator;

    private static class ObjectWithNotEmptyStringField {
        @NotEmptyString
        private String attr;

    }

    @BeforeClass(groups = "unit")
    public void setup() {
        this.notEmptyStringAnnotationValidator = new NotEmptyStringAnnotationValidator();
    }

    @Test(groups = "unit")
    public void ObjectWithNotEmptyStringField_fieldHasNotEmptyString_validatorReturnsTrue() throws NoSuchFieldException {
        Annotation notEmptyStringAnnotation = ObjectWithNotEmptyStringField.class.getDeclaredField("attr")
                .getAnnotation(NotEmptyString.class);
        assertTrue(this.notEmptyStringAnnotationValidator.validate(NOT_EMPTY_STRING, notEmptyStringAnnotation));
    }

    @Test(groups = "unit")
    public void ObjectWithNotEmptyStringField_fieldHasEmptyString_validatorReturnsFalse() throws NoSuchFieldException {
        Annotation notEmptyStringAnnotation = ObjectWithNotEmptyStringField.class.getDeclaredField("attr")
                .getAnnotation(NotEmptyString.class);
        assertFalse(this.notEmptyStringAnnotationValidator.validate("", notEmptyStringAnnotation));
    }
}
