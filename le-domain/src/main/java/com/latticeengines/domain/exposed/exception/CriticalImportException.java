package com.latticeengines.domain.exposed.exception;

public class CriticalImportException extends RuntimeException {

    private static final long serialVersionUID = 3464573272747541690L;

    public CriticalImportException(String message) {
        super(message);
    }
}
