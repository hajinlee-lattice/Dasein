package com.latticeengines.camille;

public class DocumentSerializationException extends Exception {
    private static final long serialVersionUID = 1L;

    public DocumentSerializationException() {
        super();
    }

    public DocumentSerializationException(String message) {
        super(message);
    }
   
    public DocumentSerializationException(String message, Exception inner) {
        super(message, inner);
    }
}
