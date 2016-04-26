package com.latticeengines.domain.exposed.exception;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;

public class RemoteLedpException extends LedpException {

    private HttpStatus httpStatus;

    public RemoteLedpException(HttpStatus httpStatus, LedpCode code, Throwable remoteException) {
        super(code, remoteException);
        this.httpStatus = httpStatus;
    }

    public RemoteLedpException(HttpStatus httpStatus, LedpCode code, String msg, Throwable remoteException) {
        super(code, msg, remoteException);
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    @Override
    public ErrorDetails getErrorDetails() {
        String stackTrace = ExceptionUtils.getStackTrace(this);

        Throwable remote = getCause();
        if (remote != null) {
            stackTrace += "\nCaused remotely by...\n";
        }
        stackTrace += remote.getMessage() + "\n";
        stackTrace += ExceptionUtils.getFullStackTrace(remote);
        return new ErrorDetails(getCode(), getMessage(), stackTrace);
    }
}
