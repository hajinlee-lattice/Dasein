package com.latticeengines.common.exposed.util;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;

import springfox.documentation.RequestHandler;

public class SwaggerUtils {

    public static Predicate<RequestHandler> getApiSelector(final String ... classCanonicalNameRegex) {
        return new Predicate<RequestHandler>() {
            @SuppressWarnings("deprecation")
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
