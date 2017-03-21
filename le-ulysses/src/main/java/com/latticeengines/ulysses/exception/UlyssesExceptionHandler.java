package com.latticeengines.ulysses.exception;

import org.springframework.web.bind.annotation.ControllerAdvice;

import com.latticeengines.security.exposed.serviceruntime.exception.FrontEndFacingExceptionHandler;

@ControllerAdvice
public class UlyssesExceptionHandler extends FrontEndFacingExceptionHandler {
}
