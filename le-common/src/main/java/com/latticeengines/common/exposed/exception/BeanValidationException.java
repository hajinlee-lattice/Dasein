package com.latticeengines.common.exposed.exception;

public class BeanValidationException extends RuntimeException {

    private static final long serialVersionUID = -438673119455606030L;

    public BeanValidationException(String message) {
        super(message);
    }
}
