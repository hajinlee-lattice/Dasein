package com.latticeengines.common.exposed.validator;

import java.util.Set;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;

public interface BeanValidationService {
    Set<AnnotationValidationError> validate(Object bean) throws Exception;
}
