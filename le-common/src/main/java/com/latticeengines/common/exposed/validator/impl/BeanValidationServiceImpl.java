package com.latticeengines.common.exposed.validator.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.AnnotationValidator;
import com.latticeengines.common.exposed.validator.BeanValidationService;

public class BeanValidationServiceImpl implements BeanValidationService {
    private static final Log log = LogFactory.getLog(BeanValidationServiceImpl.class);

    private static final String ANNOTATION_VALIDATOR = "AnnotationValidator";
    private static final String ANNOTATION_VALIDATOR_CLASS_PATH = "com.latticeengines.common.exposed.validator.impl.%s";

    private static final Map<String, AnnotationValidator> nameToAnnotationValidators = new ConcurrentHashMap<>();

    @Override
    public Set<AnnotationValidationError> validate(final Object bean) {
        final Set<AnnotationValidationError> errorSet = new HashSet<AnnotationValidationError>();

        ReflectionUtils.doWithFields(bean.getClass(), new FieldCallback() {
            @Override
            public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                Object fieldValue = BeanValidationServiceImpl.this.getValueForField(field, bean);
                Annotation[] annotations = field.getAnnotations();
                for (Annotation annotation : annotations) {
                    String annotationValidatorName = BeanValidationServiceImpl.this
                            .getAnnotationValidatorName(annotation);
                    try {
                        if (!nameToAnnotationValidators.containsKey(annotationValidatorName)) {
                            nameToAnnotationValidators.put(annotationValidatorName, BeanValidationServiceImpl.this
                                    .getAnnotationValidatorFromValidatorName(annotationValidatorName));
                        }
                        AnnotationValidator annotationValidator = nameToAnnotationValidators
                                .get(annotationValidatorName);
                        if (!annotationValidator.validate(fieldValue, annotation)) {
                            AnnotationValidationError error = new AnnotationValidationError(field.getName(),
                                    annotationValidatorName);
                            errorSet.add(error);
                        }
                    } catch (ClassNotFoundException e) {
                        log.info(String.format("%s does not have one of the declared annotations", annotation
                                .getClass().getName()));
                    }
                }
            }
        });

        return errorSet;
    }

    private AnnotationValidator getAnnotationValidatorFromValidatorName(String validatorName)
            throws ClassNotFoundException {
        try {
            Class<?> annotationValidatorClass = Class.forName(String.format(ANNOTATION_VALIDATOR_CLASS_PATH,
                    validatorName));
            return (AnnotationValidator) annotationValidatorClass.newInstance();
        } catch (IllegalAccessException e) {
            return null;
        } catch (InstantiationException e) {
            return null;
        }
    }

    private String getAnnotationValidatorName(Annotation annotation) {
        String annotationName = annotation.annotationType().getName();
        String[] annotationSplitName = annotationName.split("\\.");
        return annotationSplitName[annotationSplitName.length - 1].concat(ANNOTATION_VALIDATOR);
    }

    private Object getValueForField(Field field, Object bean) {
        ReflectionUtils.makeAccessible(field);
        return ReflectionUtils.getField(field, bean);
    }
}
