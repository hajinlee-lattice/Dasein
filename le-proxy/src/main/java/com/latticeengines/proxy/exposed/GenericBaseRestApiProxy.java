package com.latticeengines.proxy.exposed;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

public abstract class GenericBaseRestApiProxy extends CommonRestApiProxy {

    private RestTemplate restTemplate = new RestTemplate();

    protected GenericBaseRestApiProxy(String rootpath, Object... urlVariables) {
        this.rootpath = rootpath == null ? "" : new UriTemplate(rootpath).expand(urlVariables).toString();
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        restTemplate.setErrorHandler(new GetResponseErrorHandler());
    }

    public GenericBaseRestApiProxy() {
        this(null);
    }

    protected String constructUrl(String serviceHostPort, Object path, Object... urlVariables) {
        return super.constructUrl(serviceHostPort, path, urlVariables);
    }

    protected String constructQuartzUrl(String quartzHostPort, Object path, Object... urlVariables) {
        return constructQuartzUrl(quartzHostPort, path, urlVariables);
    }
}
