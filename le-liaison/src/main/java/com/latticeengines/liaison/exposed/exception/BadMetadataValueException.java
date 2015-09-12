package com.latticeengines.liaison.exposed.exception;

import java.io.Serializable;

public class BadMetadataValueException extends RuntimeException implements Serializable {

	private static final long serialVersionUID = -7534816217220800477L;

	public BadMetadataValueException( String message ) {
        super( message );
    }
}
