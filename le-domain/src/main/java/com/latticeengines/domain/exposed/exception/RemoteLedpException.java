package com.latticeengines.domain.exposed.exception;

import org.springframework.http.HttpStatus;

public class RemoteLedpException extends LedpException {

    private static final long serialVersionUID = -5707442145945244202L;
    
	private String remoteStackTrace;
    private HttpStatus httpStatus;

    public RemoteLedpException(String remoteStackTrace, HttpStatus httpStatus, LedpCode code) {
        super(code);
        this.remoteStackTrace = remoteStackTrace;
        this.httpStatus = httpStatus;
    }

    public RemoteLedpException(String remoteStackTrace, HttpStatus httpStatus, LedpCode code, Throwable t) {
        super(code, t);
        this.remoteStackTrace = remoteStackTrace;
        this.httpStatus = httpStatus;
    }

    public RemoteLedpException(String remoteStackTrace, HttpStatus httpStatus, LedpCode code, String msg) {
        super(code, msg, null);
        this.remoteStackTrace = remoteStackTrace;
        this.httpStatus = httpStatus;
    }

    public RemoteLedpException(String remoteStackTrace, HttpStatus httpStatus, LedpCode code, String msg, Throwable t) {
        super(code, msg, t);
        this.remoteStackTrace = remoteStackTrace;
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getRemoteStackTrace() {
        return remoteStackTrace;
    }

    public void setRemoteStackTrace(String remoteStackTrace) {
        this.remoteStackTrace = remoteStackTrace;
    }

    @Override
    public ErrorDetails getErrorDetails() {
        ErrorDetails details = super.getErrorDetails();
        details.setStackTrace(details.getStackTrace() + "\nCaused remotely by...\n" + remoteStackTrace);
        return details;
    }

}
