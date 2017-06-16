package com.latticeengines.proxy.exposed;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResponseErrorHandler;

@Component("restApiClient")
@Scope("prototype")
public class RestApiClient extends BaseRestApiProxy {

    // Used to call external API because there is no standardized error handler
    public RestApiClient() {
        super();
    }

    public RestApiClient(String hostport) {
        super(hostport);
        cleanupAuthHeader();
    }

    /**
     * This is the client used to talk to lattice's internal servers.
     * This client will ignore ssl name check
     * @param appCtx
     * @param hostport
     * @return RestApiClient
     */
    public static RestApiClient newInternalClient(ApplicationContext appCtx, String hostport) {
        return (RestApiClient) appCtx.getBean("restApiClient", hostport);
    }

    /**
     * This is the client used to talk to servers outside of lattice.
     * This client WON'T ignore ssl name check
     * @param appCtx
     * @param hostport
     * @return RestApiClient
     */
    public static RestApiClient newExternalClient(ApplicationContext appCtx, String hostport) {
        RestApiClient restApiClient = (RestApiClient) appCtx.getBean("restApiClient", hostport);
        restApiClient.enforceSSLNameVerification();
        return restApiClient;
    }

    /**
     * This is the client used to call external api outside of lattice. This
     * client ignores ssl name check This client WON'T use standard error
     * handler in BaseRestApiProxy
     * 
     * @param appCtx
     * @param hostport
     * @return RestApiClient
     */
    public static RestApiClient newExternalClient(ApplicationContext appCtx) {
        RestApiClient restApiClient = (RestApiClient) appCtx.getBean("restApiClient");
        return restApiClient;
    }

    public void setErrorHandler(ResponseErrorHandler handler) {
        super.setErrorHandler(handler);
    }

    public String get(final String path, final String... variables) {
        return get(String.class, path, variables);
    }

    public String get(final HttpEntity<String> entity, final String url) {
        return get(String.class, entity, url);
    }

    public <T> T get(final Class<T> returnValueClazz, final String path, final String... variables) {
        String fullUrl = constructUrl(path, (Object[]) variables);
        return super.get("generic get", fullUrl, returnValueClazz);
    }

    public <T> T get(final Class<T> returnValueClazz, final HttpEntity<?> entity, final String url) {
        return super.get("generic get with headers", url, entity, returnValueClazz);
    }

    public String post(final HttpEntity<String> entity, final String url) {
        return post(String.class, entity, url);
    }

    public <T> T post(final Class<T> returnValueClazz, final HttpEntity<String> entity, final String url) {
        return super.postForEntity("generic post", url, entity, returnValueClazz);
    }
}
