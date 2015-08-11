package com.latticeengines.common.exposed.validator;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.AllowedValues;
import com.latticeengines.common.exposed.validator.impl.AllowedValuesAnnotationValidator;

public class AllowedValuesAnnotationValidatorUnitTestNG {
    private static final String ALLOWED_VALUE_1 = "allowedValue1";
    private static final String ALLOWED_VALUE_2 = "allowedValue2";
    private static final String NOT_ALLOWED_VALUE = "notAllowedValue";

    private AllowedValuesAnnotationValidator allowedValuesAnnotationValidator;

    @BeforeClass(groups = "unit")
    public void setup() {
        this.allowedValuesAnnotationValidator = new AllowedValuesAnnotationValidator();
    }

    private static class ObjectWithAllowedValueAnnotations {
        @AllowedValues(values = { ALLOWED_VALUE_1 })
        private String attr1;

        @AllowedValues(values = { ALLOWED_VALUE_1, ALLOWED_VALUE_2 })
        private String attr2;

        @AllowedValues(values = { ALLOWED_VALUE_1, ALLOWED_VALUE_2 })
        private Collection<String> attr3;
    }

    @Test(groups = "unit")
    public void objectWithAllowedValuesAnnotations_fieldIsNull_validatorReturnsTrue() throws NoSuchFieldException {
        Annotation allowedValuesAnnotation = ObjectWithAllowedValueAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(AllowedValues.class);
        assertTrue(this.allowedValuesAnnotationValidator.validate(null, allowedValuesAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithAllowedValuesAnnotations_fieldIsTheAllowedValue_validatorReturnsTrue()
            throws NoSuchFieldException {
        Annotation allowedValuesAnnotation = ObjectWithAllowedValueAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(AllowedValues.class);
        assertTrue(this.allowedValuesAnnotationValidator.validate(ALLOWED_VALUE_1, allowedValuesAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithAllowedValuesAnnotations_fieldIsNotTheAllowedValue_validatorReturnsFalse()
            throws NoSuchFieldException {
        Annotation allowedValuesAnnotation = ObjectWithAllowedValueAnnotations.class.getDeclaredField("attr1")
                .getAnnotation(AllowedValues.class);
        assertFalse(this.allowedValuesAnnotationValidator.validate(NOT_ALLOWED_VALUE, allowedValuesAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithAllowedValuesAnnotations_fieldIsOneOfTheAllowedValues_validatorReturnsTrue()
            throws NoSuchFieldException {
        Annotation allowedValuesAnnotation = ObjectWithAllowedValueAnnotations.class.getDeclaredField("attr2")
                .getAnnotation(AllowedValues.class);
        assertTrue(this.allowedValuesAnnotationValidator.validate(ALLOWED_VALUE_2, allowedValuesAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithAllowedValuesAnnotations_fieldIsCollectionWithTheAllowedValues_validatorReturnsTrue()
            throws NoSuchFieldException {
        String[] allowedArray = new String[] { ALLOWED_VALUE_1, ALLOWED_VALUE_2 };
        Annotation allowedValuesAnnotation = ObjectWithAllowedValueAnnotations.class.getDeclaredField("attr3")
                .getAnnotation(AllowedValues.class);
        assertTrue(this.allowedValuesAnnotationValidator.validate(Arrays.asList(allowedArray), allowedValuesAnnotation));
    }

    @Test(groups = "unit")
    public void objectWithAllowedValuesAnnotations_fieldIsCollectionWithNotAllowedValues_validatorReturnsFalse()
            throws NoSuchFieldException {
        String[] notAllowedArray = new String[] { ALLOWED_VALUE_1, NOT_ALLOWED_VALUE };
        Annotation allowedValuesAnnotation = ObjectWithAllowedValueAnnotations.class.getDeclaredField("attr3")
                .getAnnotation(AllowedValues.class);
        assertFalse(this.allowedValuesAnnotationValidator.validate(Arrays.asList(notAllowedArray),
                allowedValuesAnnotation));
    }
}
