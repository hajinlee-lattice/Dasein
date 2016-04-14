package com.latticeengines.domain.exposed.exception;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class LoginException extends RuntimeException {

    private static final long serialVersionUID = -203743712473813309L;
    
    private LedpCode code;
    
    public LoginException(LedpException ledpException) {
        super(ledpException.getMessage(), ledpException.getCause());
        code = ledpException.getCode();
    }
    
    public LedpCode getCode() {
        return code;
    }
    
}
