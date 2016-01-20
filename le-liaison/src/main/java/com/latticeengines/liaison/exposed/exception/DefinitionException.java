package com.latticeengines.liaison.exposed.exception;

import java.io.Serializable;

public class DefinitionException extends RuntimeException implements Serializable {

    private static final long serialVersionUID = 5911100121661527109L;

    public DefinitionException(String message) {
        super(message);
    }
}
