package com.latticeengines.workflowapi.exception;

import com.latticeengines.security.exposed.serviceruntime.exception.InternalServiceExceptionHandler;
import org.springframework.web.bind.annotation.ControllerAdvice;

@ControllerAdvice
public class WorkflowApiExceptionHandler extends InternalServiceExceptionHandler {
}
