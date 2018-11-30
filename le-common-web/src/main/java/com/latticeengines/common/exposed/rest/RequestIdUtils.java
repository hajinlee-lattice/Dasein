package com.latticeengines.common.exposed.rest;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;

public class RequestIdUtils {

    public static final String IDENTIFIER_KEY = "com.latticeengines.requestid";
    public static final String REQUEST_ID = "Request-Id";

    public static String getRequestIdentifierId(HttpServletRequest request) {
        String requestId = "";
        Object identifier = request.getAttribute(RequestIdUtils.IDENTIFIER_KEY);
        if (!StringStandardizationUtils.objectIsNullOrEmptyString(identifier)) {
            requestId = String.valueOf(identifier);
        }
        return requestId;
    }
}
