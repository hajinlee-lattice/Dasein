package com.latticeengines.security.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

public abstract class BaseRestApiProxy {

    protected RestTemplate restTemplate = new RestTemplate();

    public abstract String getRestApiHostPort();

    public BaseRestApiProxy() {
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
    }

    protected String constructUrl(String... parts) {
        String end = combine(parts);
        return combine(getRestApiHostPort(), end);
    }

    private String combine(String... parts) {
        for (int i = 0; i < parts.length; ++i) {
            String part = parts[i];

            if (i != 0) {
                if (part.startsWith("/")) {
                    parts[i] = part.substring(1);
                }
            }

            if (i != parts.length - 1) {
                if (part.endsWith("/")) {
                    parts[i] = part.substring(0, part.length() - 2);
                }
            }
        }
        return StringUtils.join(parts, "/");
    }

}
