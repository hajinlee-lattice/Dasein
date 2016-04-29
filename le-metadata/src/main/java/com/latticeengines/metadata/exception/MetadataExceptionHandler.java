package com.latticeengines.metadata.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.security.exposed.serviceruntime.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class MetadataExceptionHandler extends InternalServiceExceptionHandler {
}
