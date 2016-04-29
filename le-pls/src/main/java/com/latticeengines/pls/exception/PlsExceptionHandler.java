package com.latticeengines.pls.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.security.exposed.serviceruntime.exception.FrontEndFacingExceptionHandler;

@ControllerAdvice
public class PlsExceptionHandler extends FrontEndFacingExceptionHandler {
}
