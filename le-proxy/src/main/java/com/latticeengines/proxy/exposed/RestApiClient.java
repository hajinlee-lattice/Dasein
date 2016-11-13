package com.latticeengines.proxy.exposed;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("restApiClient")
@Scope("prototype")
public class RestApiClient extends BaseRestApiProxy {

    public RestApiClient(String hostport) {
        super(hostport);
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

    public String get(final String path, final String... variables) {
        return get(String.class, path, variables);
    }

    public <T> T get(final Class<T> returnValueClazz, final String path, final String... variables) {
        String fullUrl = constructUrl(path, (Object[]) variables);
        return super.get("generic get", fullUrl, returnValueClazz);
    }

}
