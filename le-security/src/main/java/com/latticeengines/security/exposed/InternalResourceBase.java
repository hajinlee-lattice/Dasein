package com.latticeengines.security.exposed;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.security.exposed.exception.LoginException;

public class InternalResourceBase {

    public void checkHeader(HttpServletRequest request) {
        String value = request.getHeader(Constants.INTERNAL_SERVICE_HEADERNAME);
        
        if (value == null || !value.equals(Constants.INTERNAL_SERVICE_HEADERVALUE)) {
            throw new LoginException(new LedpException(LedpCode.LEDP_18001, new String[] {}));
        }
    }
}
