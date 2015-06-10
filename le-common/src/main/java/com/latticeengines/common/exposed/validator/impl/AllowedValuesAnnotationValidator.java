package com.latticeengines.common.exposed.validator.impl;

import java.lang.annotation.Annotation;
import java.util.Arrays;

import com.latticeengines.common.exposed.validator.AnnotationValidator;
import com.latticeengines.common.exposed.validator.annotation.AllowedValues;

public class AllowedValuesAnnotationValidator implements AnnotationValidator {
    @Override
    public boolean validate(Object valueToValidate, Annotation annotation) {
        String[] allowedValues = ((AllowedValues) annotation).values();
        String value = (String) valueToValidate;
        if (value == null || Arrays.asList(allowedValues).contains(value)) {
            return true;
        }
        return false;
    }
}
