package com.latticeengines.proxy.exposed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;

public abstract class ProtectedRestApiProxy extends BaseRestApiProxy {

    private static final Log log = LogFactory.getLog(ProtectedRestApiProxy.class);

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
