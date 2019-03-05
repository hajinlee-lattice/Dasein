package com.latticeengines.microservice.exposed;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.security.exposed.serviceruntime.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class MicroserviceExceptionHandler extends InternalServiceExceptionHandler {
}
