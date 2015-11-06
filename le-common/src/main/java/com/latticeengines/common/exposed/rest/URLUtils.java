package com.latticeengines.common.exposed.rest;

public class URLUtils {

    public static String getRestAPIHostPort(String hostPort) {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

}
