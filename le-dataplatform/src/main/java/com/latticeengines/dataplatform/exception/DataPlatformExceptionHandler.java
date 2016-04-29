package com.latticeengines.dataplatform.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.security.exposed.serviceruntime.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class DataPlatformExceptionHandler extends InternalServiceExceptionHandler {
}
