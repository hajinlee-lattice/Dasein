package com.latticeengines.common.exposed.util;

import javax.annotation.Nullable;

import springfox.documentation.RequestHandler;

import com.google.common.base.Predicate;

public class SwaggerUtils {

    public static Predicate<RequestHandler> getApiSelector(final String ... classCanonicalNameRegex) {
        return new Predicate<RequestHandler>() {
            @Override
            public boolean apply(@Nullable RequestHandler requestHandler) {
                if (requestHandler != null && requestHandler.getRequestMapping() != null) {
                    String canonicalName = requestHandler.getHandlerMethod().getMethod().getDeclaringClass().getCanonicalName();
                    for (String pattern : classCanonicalNameRegex) {
                        if (canonicalName.matches(pattern)) {
                            return true;
                        }
                    }
                }
                return false;
            }
        };
    }

}
