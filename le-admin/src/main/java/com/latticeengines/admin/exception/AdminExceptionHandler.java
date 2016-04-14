package com.latticeengines.admin.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.serviceruntime.exposed.exception.FrontEndFacingExceptionHandler;

@ControllerAdvice
public class AdminExceptionHandler extends FrontEndFacingExceptionHandler {
}
