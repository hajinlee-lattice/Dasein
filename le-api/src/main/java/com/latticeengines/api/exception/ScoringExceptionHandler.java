package com.latticeengines.api.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.security.exposed.serviceruntime.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class ScoringExceptionHandler extends InternalServiceExceptionHandler {
}
