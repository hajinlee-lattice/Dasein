package com.latticeengines.liaison.exposed.exception;

import java.io.Serializable;

public class UnknownDataLoaderObjectException extends RuntimeException implements Serializable {

    private static final long serialVersionUID = -1312673706507117281L;

    public UnknownDataLoaderObjectException(String message) {
        super(message);
    }
}
