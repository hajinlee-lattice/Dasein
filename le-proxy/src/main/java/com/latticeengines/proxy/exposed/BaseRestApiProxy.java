package com.latticeengines.proxy.exposed;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

public abstract class BaseRestApiProxy {
    private RestTemplate restTemplate = new RestTemplate();
    private static final Log log = LogFactory.getLog(BaseRestApiProxy.class);
    private String rootpath;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    public BaseRestApiProxy(String rootpath, Object... urlVariables) {
        this.rootpath = rootpath == null ? "" : new UriTemplate(rootpath).expand(urlVariables).toString();
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
    }

    public BaseRestApiProxy() {
        this(null);
    }

    protected <T, B> T post(String method, String url, B body, Class<T> clazz) {
        log.info(String.format("Invoking %s by posting to url %s with body %s", method, url, body));
        try {
            return restTemplate.postForObject(url, body, clazz);
        } catch (Exception e) {
            throw new RuntimeException(String.format("%s: Remote call failure", method), e);
        }
    }

    protected <T> T get(String method, String url, Class<T> clazz) {
        log.info(String.format("Invoking %s by getting from url %s", method, url));
        try {
            return restTemplate.getForObject(url, clazz);
        } catch (Exception e) {
            throw new RuntimeException(String.format("%s: Remote call failure", method), e);
        }
    }

    protected String constructUrl() {
        return constructUrl(null);
    }

    protected String constructUrl(Object path, Object... urlVariables) {
        if (microserviceHostPort == null || microserviceHostPort.equals("")) {
            throw new NullPointerException("microserviceHostPort must be set");
        }
        String end = rootpath;
        if (path != null) {
            String expandedPath = new UriTemplate(path.toString()).expand(urlVariables).toString();
            end = combine(rootpath, expandedPath);
        }
        return combine(microserviceHostPort, end);
    }

    private String combine(Object... parts) {
        List<String> toCombine = new ArrayList<>();
        for (int i = 0; i < parts.length; ++i) {
            String part = parts[i].toString();
            if (i != 0) {
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
            }

            if (i != parts.length - 1) {
                if (part.endsWith("/")) {
                    part = part.substring(0, part.length() - 2);
                }
            }
            toCombine.add(part);
        }
        return StringUtils.join(toCombine, "/");
    }

    public String getMicroserviceHostPort() {
        return microserviceHostPort;
    }
}
