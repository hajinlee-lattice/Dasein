package com.latticeengines.proxy.exposed;

public abstract class ProtectedRestApiProxy extends BaseRestApiProxy {

    protected ProtectedRestApiProxy(String hostport, String rootpath, Object... urlVariables) {
        super(hostport, rootpath, urlVariables);
    }

    public void login(String username, String password) {
        String token = loginInternal(username, password);
        setAuthHeader(token);
    }

    protected abstract String loginInternal(String username, String password);

}
