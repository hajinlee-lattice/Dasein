package com.latticeengines.common.exposed.util;

import java.util.function.Predicate;

import springfox.documentation.RequestHandler;

public class SwaggerUtils {

    public static Predicate<RequestHandler> getApiSelector(final String ... classCanonicalNameRegex) {
        return (requestHandler) -> {
            if (requestHandler != null && requestHandler.getRequestMapping() != null) {
                String canonicalName = requestHandler.getHandlerMethod().getMethod().getDeclaringClass().getCanonicalName();
                for (String pattern : classCanonicalNameRegex) {
                    if (canonicalName.matches(pattern)) {
                        return true;
                    }
                }
            }
            return false;
        };
    }

}
