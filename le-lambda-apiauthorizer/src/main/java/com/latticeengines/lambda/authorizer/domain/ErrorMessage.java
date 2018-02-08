package com.latticeengines.lambda.authorizer.domain;

public class ErrorMessage {

    int httpStatus;
    
    String errorType;
    
    String message;

    public ErrorMessage() {
        
    }

    public ErrorMessage(int httpStatus, String errorType) {
        this.httpStatus = httpStatus;
        this.errorType = errorType;
    }
    
    public ErrorMessage(int httpStatus, String errorType, String message) {
        this(httpStatus, errorType);
        this.message = message;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
    
}
