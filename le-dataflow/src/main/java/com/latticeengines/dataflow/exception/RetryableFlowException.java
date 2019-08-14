package com.latticeengines.dataflow.exception;

public class RetryableFlowException extends RuntimeException {

    public RetryableFlowException(Throwable t) {
        super(t);
    }

}
