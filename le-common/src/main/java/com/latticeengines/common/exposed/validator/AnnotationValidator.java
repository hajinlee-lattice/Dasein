package com.latticeengines.common.exposed.validator;

import java.lang.annotation.Annotation;

public interface AnnotationValidator {
    boolean validate(Object valueToValidate, Annotation annotation);
}
