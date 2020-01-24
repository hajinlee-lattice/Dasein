package com.latticeengines.common.exposed.util;

public final class URLUtils {

    protected URLUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getRestAPIHostPort(String hostPort) {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

}
