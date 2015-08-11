package com.latticeengines.common.exposed.validator.impl;

import java.lang.annotation.Annotation;

import com.latticeengines.common.exposed.validator.AnnotationValidator;

public class NotEmptyStringAnnotationValidator implements AnnotationValidator {

    @Override
    public boolean validate(Object valueToValidate, Annotation annotation) {
        if (valueToValidate instanceof String && valueToValidate.equals("")) {
            return false;
        }
        return true;
    }

}
