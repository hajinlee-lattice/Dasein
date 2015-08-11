package com.latticeengines.common.exposed.validator.impl;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;

import com.latticeengines.common.exposed.validator.AnnotationValidator;
import com.latticeengines.common.exposed.validator.annotation.AllowedValues;

public class AllowedValuesAnnotationValidator implements AnnotationValidator {
    @Override
    public boolean validate(Object valueToValidate, Annotation annotation) {
        String[] allowedValues = ((AllowedValues) annotation).values();
        if (valueToValidate == null) {
            return true;
        } else if (valueToValidate instanceof String) {
            return valueIsAllowed(valueToValidate, allowedValues);
        } else {
            int sizeOfArray = ((Collection<?>) valueToValidate).size();
            return valuesAreAllowed(((Collection<?>) valueToValidate).toArray(new Object[sizeOfArray]), allowedValues);
        }
    }

    private boolean valuesAreAllowed(Object[] values, String[] allowedValues) {
        if (values.length == 0) {
            return true;
        }
        for (Object value : values) {
            if (!valueIsAllowed(value, allowedValues)) {
                return false;
            }
        }
        return true;
    }

    private boolean valueIsAllowed(Object value, String[] allowedValues) {
        return Arrays.asList(allowedValues).contains(value.toString());
    }

}
