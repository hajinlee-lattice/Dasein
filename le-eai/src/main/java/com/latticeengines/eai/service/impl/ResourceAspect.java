package com.latticeengines.eai.service.impl;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.InternalResourceBase;

@Aspect
public class ResourceAspect {

    @Autowired
    private BeanValidationService beanValidationService;

    private InternalResourceBase internalResourceBase = new InternalResourceBase();

    @Before("execution(* com.latticeengines.eai.controller.EaiResource.*(..))")
    public void allMethodsForSubmitEaiJobResource(JoinPoint joinPoint) {
        checkHeader(joinPoint);
        EaiJobConfiguration eaiJobConfig = (EaiJobConfiguration) joinPoint.getArgs()[0];
        validateJobConfig(eaiJobConfig);
    }

    private void validateJobConfig(EaiJobConfiguration eaiJobConfig) {
        Set<AnnotationValidationError> validationErrors = beanValidationService.validate(eaiJobConfig);
        if (validationErrors.size() > 0) {
            StringBuilder validationErrorStringBuilder = new StringBuilder();
            for (AnnotationValidationError annotationValidationError : validationErrors) {
                validationErrorStringBuilder.append(annotationValidationError.getFieldName() + ":"
                        + annotationValidationError.getAnnotationName() + "\n");
            }

            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { eaiJobConfig.toString(), validationErrorStringBuilder.toString() });
        }
    }

    private void checkHeader(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();

        for (Object arg : args) {
            if (arg instanceof HttpServletRequest) {
                internalResourceBase.checkHeader((HttpServletRequest) arg);
            }
        }
    }

}
