package com.latticeengines.eai.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.serviceruntime.exposed.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class EaiExceptionHandler extends InternalServiceExceptionHandler {
}
