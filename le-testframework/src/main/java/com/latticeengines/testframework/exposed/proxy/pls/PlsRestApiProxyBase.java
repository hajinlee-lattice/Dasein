package com.latticeengines.testframework.exposed.proxy.pls;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;

public abstract class PlsRestApiProxyBase extends ProtectedRestApiProxy {
    private static String plsHostPort = PropertyUtils.getProperty("common.test.pls.url");

    public static void setHostPort(String hostport) {
        plsHostPort = hostport;
    }

    public PlsRestApiProxyBase(String rootpath, Object... urlVariables) {
        super(plsHostPort, rootpath, urlVariables);
    }

    @Override
    protected String loginInternal(String username, String password) {
        throw new UnsupportedOperationException("We do not support login on this proxy yet.");
    }
}
