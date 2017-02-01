package com.latticeengines.scoringapi.exposed;

import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;

public class ScoringApiRequestLogInterceptor extends RequestLogInterceptor {

    @Override
    protected String getRequestId(HttpServletRequest request) {
        String identifier = request.getHeader(REQUEST_ID);

        if (StringStandardizationUtils.objectIsNullOrEmptyString(identifier)) {
            identifier = UUID.randomUUID().toString();
        }

        return identifier;
    }

}
