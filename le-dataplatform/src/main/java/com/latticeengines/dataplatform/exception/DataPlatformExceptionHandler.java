package com.latticeengines.dataplatform.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.serviceruntime.exposed.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class DataPlatformExceptionHandler extends InternalServiceExceptionHandler {
}
