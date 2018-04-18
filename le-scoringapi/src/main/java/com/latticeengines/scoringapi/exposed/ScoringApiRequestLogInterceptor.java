package com.latticeengines.scoringapi.exposed;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.common.exposed.rest.RequestLogInterceptor;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;

public class ScoringApiRequestLogInterceptor extends RequestLogInterceptor {

    private static final List<String> skipLoggingForUris = //
            Arrays.asList( //
                    "score/record", //
                    "scoreinternal/record");

    @Override
    protected String getRequestId(HttpServletRequest request) {
        String identifier = request.getHeader(REQUEST_ID);

        if (StringStandardizationUtils.objectIsNullOrEmptyString(identifier)) {
            identifier = UUID.randomUUID().toString();
        }

        return identifier;
    }

    @Override
    protected boolean shouldSkipLogging(String requestURI) {
        Optional<String> result = skipLoggingForUris.stream() //
                .filter(skipUri -> requestURI.contains(skipUri)) //
                .findAny();
        return result.isPresent();
    }
}
