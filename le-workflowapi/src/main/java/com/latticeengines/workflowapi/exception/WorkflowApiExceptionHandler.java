package com.latticeengines.workflowapi.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.serviceruntime.exposed.exception.InternalServiceExceptionHandler;

@ControllerAdvice
public class WorkflowApiExceptionHandler extends InternalServiceExceptionHandler {
}
