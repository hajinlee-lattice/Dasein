package com.latticeengines.proxy.exposed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;

public abstract class ProtectedRestApiProxy extends BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(ProtectedRestApiProxy.class);

    protected ProtectedRestApiProxy(String hostport, String rootpath, Object... urlVariables) {
        super(hostport, rootpath, urlVariables);
        setMaxAttempts(1);
    }

    public void login(String username, String password) {
        String token = loginInternal(username, password);
        setAuthHeader(token);
        log.info("Put a new token in auth header as the user " + username); // don't put pw in this log
    }

    public void attachInterceptor(AuthorizationHeaderHttpRequestInterceptor authHeader) {
        setAuthInterceptor(authHeader);
    }

    protected abstract String loginInternal(String username, String password);

}
