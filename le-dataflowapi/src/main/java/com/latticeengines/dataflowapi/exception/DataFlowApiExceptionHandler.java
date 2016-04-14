package com.latticeengines.dataflowapi.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.serviceruntime.exposed.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class DataFlowApiExceptionHandler extends InternalServiceExceptionHandler {
}
